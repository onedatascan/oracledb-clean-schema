from __future__ import annotations

import base64
from collections import defaultdict
import json
import os
from http import HTTPStatus
from typing import Final, Protocol, TypeAlias, TypedDict, cast, runtime_checkable

from aws_lambda_powertools import Logger
from aws_lambda_powertools.logging.utils import copy_config_to_registered_loggers
from aws_lambda_powertools.utilities import parameters
from aws_lambda_powertools.utilities.parser import (
    BaseModel,
    ValidationError,
    event_parser,
    models,
    parse,
    root_validator,
)
from aws_lambda_powertools.utilities.parser.pydantic import Extra, Json, SecretStr
from aws_lambda_powertools.utilities.typing import LambdaContext

from oracledb_clean_schema.constants import SERVICE_NAME
from oracledb_clean_schema.core import drop_all

logger = Logger(service=SERVICE_NAME, level=os.getenv("LOG_LEVEL", "INFO"))
copy_config_to_registered_loggers(logger)

ENVELOPE: Final[str | None] = os.getenv("ENVELOPE")

json_types: TypeAlias = str | int | dict | list | bool | None
json_str: TypeAlias = str

HTTPResponse = TypedDict(
    "HTTPResponse",
    {
        "isBase64Encoded": bool,
        "statusCode": HTTPStatus,
        "statusDescription": str,
        "headers": dict[str, str],
        "body": json_str,
    },
)


def build_response(
    http_status: HTTPStatus, body: dict[str, json_types]
) -> HTTPResponse:
    response: HTTPResponse = {
        "isBase64Encoded": False,
        "statusCode": http_status,
        "statusDescription": f"{http_status.value} {http_status.phrase}",
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
    logger.info("Response: %s", response)
    return response


@runtime_checkable
class HTTPException(Protocol):
    http_status: HTTPStatus


class BadRequest(Exception):
    http_status = HTTPStatus.BAD_REQUEST


class Panic(Exception):
    http_status = HTTPStatus.INTERNAL_SERVER_ERROR


def format_validation_errors(e: ValidationError) -> dict[str, set]:
    logger.debug("formatting exception errors: %s", e.errors())
    reasons = defaultdict(set)

    for err in e.errors():
        field = ".".join(cast(tuple, err["loc"]))
        ctx = err.get("ctx", {})

        if err["type"] == "value_error.missing":
            reasons["possibly_missing"].add(field)
        elif err["type"] == "value_error.const":
            reasons["possibly_invalid"].add(
                (field, ctx.get("given"), ctx.get("permitted"))
            )
        else:
            reasons["other"].add((field, tuple(err.items())))
    return dict(reasons)


def exception_handler(ex: Exception, extra: dict[str, json_types] | None = None):
    logger.exception(ex, extra=extra)
    if isinstance(ex, HTTPException):
        return build_response(ex.http_status, {"exception": str(ex), "extra": extra})
    else:
        return build_response(
            HTTPStatus.INTERNAL_SERVER_ERROR, {"exception": str(ex), "extra": extra}
        )


def parse_secret(event: RequestModel) -> str:
    password = event.connection.password.get_secret_value()
    if password.startswith("arn:"):
        secret = parameters.get_secret(password, transform="json")
        password = secret["PASSWORD"]  # type: ignore
    else:
        logger.warning("Password supplied as lambda arg!")
    return password


def run_task(event, password):
    try:
        remaining_object_count = drop_all(
            event.connection.user,
            password,
            event.connection.host,
            event.connection.database,
            event.payload.target_schema,
            event.payload.parallel,
            event.payload.force,
        )
    except Exception as e:
        return exception_handler(e)

    if remaining_object_count == 0:
        logger.info(
            f"{remaining_object_count} objects remaining in "
            f"{event.payload.target_schema}"
        )
        return build_response(
            HTTPStatus.OK,
            {
                "schema": event.payload.target_schema,
                "remainingObjectCount": remaining_object_count
            }
        )
    else:
        return exception_handler(
            Panic(
                f"{remaining_object_count} objects remaining in "
                f"{event.payload.target_schema}"
            ),
            extra={
                "schema": event.payload.target_schema,
                "remainingObjectCount": remaining_object_count
            },
        )


class ConnectionModel(BaseModel):
    user: str
    password: SecretStr
    host: str
    database: str


class PayloadModel(BaseModel):
    target_schema: str
    parallel: int = int(os.getenv("PARALLEL", 1))
    force: bool = False


class RequestModel(BaseModel):
    connection: ConnectionModel
    payload: PayloadModel


class Envelope(BaseModel, extra=Extra.allow):
    body: Json[RequestModel]
    isBase64Encoded: bool

    @root_validator(pre=True)
    def prepare_data(cls, values):
        if values.get("isBase64Encoded"):
            encoded = values.get("body")
            logger.debug("Decoding base64 request body before parsing")
            payload = base64.b64decode(encoded).decode("utf-8")
            values["body"] = json.loads(json.dumps(payload))
        return values


def request_handler(event: RequestModel, context: LambdaContext) -> HTTPResponse:
    logger.debug("RequestModel: %s", repr(event))
    password = parse_secret(event)
    return run_task(event, password)


@event_parser(model=Envelope)
def envelope_handler(event: Envelope, context: LambdaContext) -> HTTPResponse:
    return request_handler(event.body, context)


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> HTTPResponse | None:
    """sample event:
    event = {
        "connection": {
            "user": "system",
            "password": "manager",
            "host": "host.docker.internal",
            "database": "orclpdb1"
        },
        "payload": {
            "target_schema": "hr2",
            "parallel": 8,
            "force": false,
        }
    }
    """
    logger.set_correlation_id(context.aws_request_id)

    envelope_validation_exc: ValidationError | None = None
    if ENVELOPE:
        # Extract the request from outer envelope supplied as an env arg. Valid args
        # could potentially be any one of:
        # https://awslabs.github.io/aws-lambda-powertools-python/2.9.1/utilities/parser/#built-in-models
        # Currently the expectation is that the outer envelope is a AlbModel or
        # APIGatewayProxyEventModel
        logger.debug("ENVELOPE=%s", ENVELOPE)
        expected_envelope = getattr(models, ENVELOPE)
        try:
            envelope_request = parse(event=event, model=expected_envelope)
            return envelope_handler(envelope_request, context)
        except ValidationError as e:
            # We might have been passed an un-enveloped request
            logger.info(
                f"Envelope validation failed for {ENVELOPE}! Attempting raw request "
                "validation..."
            )
            envelope_validation_exc = e

    try:
        return request_handler(parse(event, model=RequestModel), context)
    except ValidationError as raw_validation_exc:
        ee = (
            format_validation_errors(envelope_validation_exc)
            if envelope_validation_exc
            else None
        )
        re = format_validation_errors(raw_validation_exc)
        exc = BadRequest(
            {"RawValidationException": re, "EnvelopeValidationException": ee}
        )
        return exception_handler(exc)
