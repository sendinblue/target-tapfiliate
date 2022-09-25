#!/usr/bin/env python3

import argparse
import io
import json
import sys

import singer
from jsonschema.validators import Draft4Validator

from target_tapfiliate.tapfiliate_client import TapfiliateRestApi

logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["x-api-token"]


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    tapfiliate_client = TapfiliateRestApi(x_api_key=config["x-api-token"], retry=5)

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if "type" not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            stream = o["stream"]
            if stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        o["stream"]
                    )
                )

            # Get schema for this record's stream
            schema = schemas[stream]

            # Validate record
            # validators[stream].validate(o['record'])

            if stream == "conversions-add-commissions-to-conversion":
                tapfiliate_client.conversions_add_commissions_to_conversion(o["record"])

            state = None
        elif t == "STATE":
            logger.debug("Setting state to {}".format(o["value"]))
            state = o["value"]
        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            stream = o["stream"]

            if stream not in tapfiliate_client.tapfiliate_streams:
                raise Exception(
                    f"Stream {stream} can't be processed by target-tapfiliate"
                )

            schemas[stream] = o["schema"]
            validators[stream] = Draft4Validator(o["schema"])
            if "key_properties" not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o["key_properties"]
        else:
            raise Exception(
                "Unknown message type {} in message {}".format(o["type"], o)
            )

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    for required_config in REQUIRED_CONFIG_KEYS:
        if required_config not in config.keys():
            raise KeyError(f"Missing required config {required_config}")

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
