"""
테스트 2: 성공한 테스트 + output 테이블만 추가
"""
import os
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger("flink-test")

from pyflink.table import EnvironmentSettings, TableEnvironment


def get_application_properties():
    props_file = "/etc/flink/application_properties.json"
    if os.path.isfile(props_file):
        with open(props_file) as f:
            raw = json.load(f)
        props = {}
        for group in raw:
            group_id = group.get("PropertyGroupId", "")
            for k, v in group.get("PropertyMap", {}).items():
                props[f"{group_id}.{k}"] = v
        return props
    else:
        return {
            "kinesis.input.stream": "crypto-stream-input",
            "kinesis.output.stream": "crypto-stream-output",
            "kinesis.region": "ap-northeast-2",
            "s3.usd.krw.rate": "1370.0",
        }


def main():
    props = get_application_properties()

    env_settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    t_env = TableEnvironment.create(env_settings)

    t_env.execute_sql("""
        CREATE TABLE crypto_input (
            `exchange`   VARCHAR(20),
            symbol       VARCHAR(20),
            price        DECIMAL(20, 8),
            currency     VARCHAR(10),
            `timestamp`  VARCHAR(30),
            event_time AS TO_TIMESTAMP(
                REPLACE(`timestamp`, 'Z', ''),
                'yyyy-MM-dd''T''HH:mm:ss'
            ),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'           = 'kinesis-legacy',
            'stream'              = '%s',
            'aws.region'          = '%s',
            'scan.stream.initpos' = 'LATEST',
            'format'              = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """ % (props["kinesis.input.stream"], props["kinesis.region"]))

    t_env.execute_sql("""
        CREATE TABLE arbitrage_output (
            symbol           VARCHAR(20),
            buy_exchange     VARCHAR(20),
            buy_price_krw    DECIMAL(20, 2),
            sell_exchange    VARCHAR(20),
            sell_price_krw   DECIMAL(20, 2),
            spread_krw       DECIMAL(20, 2),
            spread_pct       DECIMAL(8, 4),
            exchange_rate    DECIMAL(15, 6),
            detected_at      TIMESTAMP(3)
        ) WITH (
            'connector'        = 'kinesis',
            'stream.arn'       = 'arn:aws:kinesis:%s:827913617635:stream/%s',
            'aws.region'       = '%s',
            'sink.partitioner' = 'random',
            'format'           = 'json'
        )
    """ % (
        props["kinesis.region"],
        props["kinesis.output.stream"],
        props["kinesis.region"],
    ))

    t_env.execute_sql("""
        INSERT INTO arbitrage_output
        SELECT
            `exchange` AS symbol,
            `exchange` AS buy_exchange,
            price      AS buy_price_krw,
            `exchange` AS sell_exchange,
            price      AS sell_price_krw,
            price      AS spread_krw,
            CAST(0 AS DECIMAL(8,4)) AS spread_pct,
            CAST(0 AS DECIMAL(15,6)) AS exchange_rate,
            CAST(event_time AS TIMESTAMP(3)) AS detected_at
        FROM crypto_input
    """).wait()


if __name__ == "__main__":
    main()