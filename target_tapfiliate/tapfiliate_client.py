import json
import time

import requests
import singer

LOGGER = singer.get_logger()


class TapfiliateRestApi(object):
    tapfiliate_streams = ["conversions-add-commissions-to-conversion"]

    def __init__(
        self,
        x_api_key,
        api_base="https://api.tapfiliate.com",
        api_version="1.6",
        retry=10,
    ):
        self.x_api_key = x_api_key
        self.api_base = api_base
        self.api_version = api_version
        self.retry = retry

    def _validate_record(
        self,
        record: dict,
        required_uri_parameters,
        required_payload,
        optional_arguments,
    ):
        uri_parameters = {}
        payload = {}

        for parameter in required_uri_parameters:
            if parameter not in record.keys():
                raise KeyError(
                    f"Missing REQUIRED_URI_PARAMETERS {parameter} in {record}"
                )
            else:
                uri_parameters[parameter] = record.pop(parameter)

        for parameter in required_payload:
            if parameter not in record.keys():
                raise KeyError(f"Missing REQUIRED_ARGUMENTS {parameter} in {record}")
            else:
                payload[parameter] = record.pop(parameter)

        for parameter in optional_arguments:
            if parameter in record.keys():
                payload[parameter] = record.pop(parameter)

        if record:
            LOGGER.warn(f"Received unexpected parameters : {record.keys()}")

        return uri_parameters, payload

    def conversions_add_commissions_to_conversion(self, record: dict):
        LOGGER.debug(f"conversions_add_commissions_to_conversion received document : {record}")

        # https://tapfiliate.com/docs/rest/#conversions-add-commissions-to-conversion
        required_uri_parameters = ["conversion_id"]
        required_arguments = ["conversion_sub_amount"]
        optional_arguments = ["commission_type", "comment"]

        uri_parameters, payload = self._validate_record(
            record, required_uri_parameters, required_arguments, optional_arguments
        )

        url = f"{self.api_base}/{self.api_version}/conversions/{uri_parameters.get('conversion_id')}/commissions/"
        response = self._sync_endpoints(url, payload)

        LOGGER.info(
            f"New commission {response} added to conversion {uri_parameters.get('conversion_id')}"
        )

    def _sync_endpoints(self, url, payload):
        # Configure call header
        headers = {"content-type": "application/json", "X-Api-Key": self.x_api_key}
        current_retry = 0

        while True:
            response = requests.post(url, headers=headers, data=payload, timeout=60)

            if response.status_code != 200:
                if current_retry < self.retry:
                    LOGGER.warning(
                        f"Unexpected response status_code {response.status_code} i need to sleep 60s before retry {current_retry}/{self.retry}"
                    )
                    time.sleep(60)
                    current_retry = current_retry + 1
                else:
                    raise RuntimeError(
                        f"Too many retry, last response status_code {response.status_code} : {response.content}"
                    )
            else:
                return json.loads(response.text)
