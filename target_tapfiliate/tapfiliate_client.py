import json
import time
import urllib

import requests
import singer

LOGGER = singer.get_logger()


class TapfiliateRestApi(object):
    tapfiliate_get_streams = [
        "affiliate-groups",
        "affiliate-prospects",
        "affiliates",
        # "balances",
        "commissions",
        "conversions",
        "customers",
        # "payments",
        "programs",
    ]

    tapfiliate_post_streams = ["conversions-add-commissions-to-conversion"]

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
            LOGGER.debug(f"Received unexpected parameters : {record.keys()}")

        return uri_parameters, payload

    def conversions_add_commissions_to_conversion(
        self, record: dict, filter_already_sent_commissions=True
    ):
        LOGGER.info(
            f"conversions_add_commissions_to_conversion received document : {record}"
        )

        # https://tapfiliate.com/docs/rest/#conversions-add-commissions-to-conversion
        required_uri_parameters = ["conversion_id"]
        required_arguments = ["conversion_sub_amount"]
        optional_arguments = ["commission_type", "comment"]

        uri_parameters, payload = self._validate_record(
            record.copy(),
            required_uri_parameters,
            required_arguments,
            optional_arguments,
        )

        if filter_already_sent_commissions:
            LOGGER.debug(
                f"Filter already sent commissions activated"
            )
            already_sent_commission_types = set()
            conversions = [
                conversion
                for _, conversion in self.get_sync_endpoints(
                    f"conversions/{uri_parameters.get('conversion_id')}"
                )
            ]
            if conversions is None:
                raise KeyError(
                    f"This conversion id not exists yet {uri_parameters.get('conversion_id')}"
                )
            else:
                LOGGER.info(
                    f"commission id {uri_parameters.get('conversion_id')} founded"
                )
                # get all already sent commission_type
                for conversion in conversions:
                    if "commissions" in conversion:
                        for commission in conversion["commissions"]:
                            already_sent_commission_types.add(
                                commission["commission_type"]
                            )
                LOGGER.info(
                    f"Already sent commission_type {already_sent_commission_types} for commission id {uri_parameters.get('conversion_id')}"
                )

            if payload.get("commission_type") in already_sent_commission_types:
                LOGGER.info(
                    f"This commission_type {payload.get('commission_type')} already exists in this conversion {uri_parameters.get('conversion_id')}"
                )
                with singer.metrics.Counter('bypassed_commissions_count') as counter:
                    counter.increment()

                return True
            else:
                LOGGER.info(
                    f"This is a new commission_type {payload.get('commission_type')} for {uri_parameters.get('conversion_id')}"
                )

        end_point = f"conversions/{uri_parameters.get('conversion_id')}/commissions/"
        response = self.post_sync_endpoints(end_point, payload)

        LOGGER.info(
            f"New commission {response} added to conversion {uri_parameters.get('conversion_id')}"
        )
        with singer.metrics.Counter('accepted_commissions_count') as counter:
            counter.increment()

        return True

    def post_sync_endpoints(self, end_point, payload):
        # Configure call header
        headers = {"content-type": "application/json", "X-Api-Key": self.x_api_key}
        current_retry = 0

        url = f"{self.api_base}/{self.api_version}/{end_point}"

        while True:
            response = requests.post(url, headers=headers, json=payload, timeout=60)

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

    def get_sync_endpoints(self, end_point, parameters={}):
        # Endpoints documentations
        # https://tapfiliate.com/docs/rest/#customers-customers-collection-get
        # https://tapfiliate.com/docs/rest/#conversions-conversions-collection-get
        # https://tapfiliate.com/docs/rest/#commissions-commissions-collection-get
        # https://tapfiliate.com/docs/rest/#affiliates-affiliates-collection-get
        # https://tapfiliate.com/docs/rest/#affiliate-groups-affiliate-group-get
        # https://tapfiliate.com/docs/rest/#affiliate-prospects-affiliate-prospects-collection-get
        # https://tapfiliate.com/docs/rest/#programs-programs-collection-get
        # https://tapfiliate.com/docs/rest/#payments-balances-collection-get
        # https://tapfiliate.com/docs/rest/#payments-payments-collection-get

        # Configure call header
        headers = {"content-type": "application/json", "X-Api-Key": self.x_api_key}

        # Set default url parameter
        if "page" not in parameters:
            parameters["page"] = 1

        is_first_call = True
        more_pages = True
        current_retry = 0
        while more_pages:
            url = f"{self.api_base}/{self.api_version}/{end_point}/?{urllib.parse.unquote(urllib.parse.urlencode(parameters))}"
            if is_first_call:
                LOGGER.info(f"Get from URL (first call) : {url}")
            else:
                LOGGER.debug(f"Get from URL : {url}")

            try:
                response = requests.get(url, headers=headers, timeout=60)

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
                    if is_first_call and "link" in response.headers:
                        # display all links
                        LOGGER.info(f"links : {response.headers['link']}")

                    records = json.loads(response.content.decode("utf-8"))
                    if isinstance(records, dict):
                        LOGGER.debug(
                            "Last call returned one document, convert it to list of one document"
                        )
                        records = [records]

                    LOGGER.info(
                        f"Last call for {end_point}, {parameters=} returned {len(records)} documents"
                    )
                    for record in records:
                        yield parameters["page"], record

                    # 25 is the max returned documents count by call
                    if len(records) < 25:
                        LOGGER.info("No need to do more calls")
                        more_pages = False
                        is_first_call = False

                    else:
                        parameters["page"] = parameters["page"] + 1
                        is_first_call = False

                        # Display next page number every xx pages
                        if parameters["page"] % 10 == 0:
                            LOGGER.info(
                                f"Next {end_point} page to get {parameters['page']}. Links : {response.headers['Link']}"
                            )

                        # The number of requests you have left before exceeding the rate limit
                        x_ratelimit_remaining = int(
                            response.headers["X-Ratelimit-Remaining"]
                        )

                        # When your number of requests will reset (Unix Timestamp in seconds)
                        x_ratelimit_reset = int(response.headers["X-Ratelimit-Reset"])

                        # if we cannot make more call : we wait until the reset
                        if x_ratelimit_remaining < 15:
                            sleep_duration = x_ratelimit_reset - time.time()
                            if sleep_duration < 30:
                                sleep_duration = 30
                            LOGGER.warning(
                                f"Remaining {x_ratelimit_remaining} call, I prefer to sleep {sleep_duration} seconds until the rest."
                            )
                            time.sleep(sleep_duration)

            except Exception as e:
                if current_retry < self.retry:
                    LOGGER.warning(
                        f"I need to sleep 60 s, Because last get call to {url} raised exception : {e}"
                    )
                    time.sleep(60)
                    current_retry = current_retry + 1
                else:
                    raise e
