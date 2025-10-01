import asyncio
import asyncpg
import json
import logging

logger = logging.getLogger("db-consumer")
import pprint


class Writer:
    runner: asyncio.Task

    def __init__(self, queue: asyncio.Queue, db: str):
        self.queue = queue
        self.db = db

    async def start(self):
        self.pool = await asyncpg.create_pool(self.db, min_size=1, max_size=2)
        logger.debug('db connected')
        self.runner = asyncio.create_task(self.run())
        logger.debug('db consumer started')
        
        def run_lost(t: asyncio.Task):
            logger.error("db loop broken")
            if e := t.exception():
                logger.critical(e)
                asyncio.create_task(self.pool.close()).add_done_callback(
                    lambda t: asyncio.create_task(self.start())
                )

        self.runner.add_done_callback(run_lost)

    async def run(self):
        while True:

            try:
                object_id, control_id, imei, records, evtype = await self.queue.get()
                evtype:int
                logger.debug(pprint.pformat([object_id, control_id, imei, len(records), f"{evtype:X}"]))
                async with self.pool.acquire() as con:
                    con: asyncpg.Connection
                    for record in records:
                        await con.execute(
                            "INSERT INTO telemetry.data(terminal_id, data) values ($1,$2)",
                            (control_id << 32) | object_id,
                            json.dumps(record),
                        )
                        valid = "8" in record and ((record["8"] & 3) == 3)
                        satcount = record.get("8", 0) >> 3

                        if record.get("12", 0) > 3000 or record.get("12", 0) < 0:
                            valid = False

                        if valid:
                            await con.execute(
                                """
                                insert into telemetry.position 
                                    (longitude, latitude, direction, navigation_time, updated, terminal_id)
                                values 
                                    ($1/ 600000.0,$2/ 600000.0,$3/ 10,to_timestamp($4),now(), $5)
                                on conflict (terminal_id) do update set 
                                    (longitude, latitude, direction, navigation_time, updated, terminal_id) 
                                      = 
                                    ($1/ 600000.0,$2/ 600000.0,$3/ 10,to_timestamp($4),now(), $5)
                                    where telemetry.position.navigation_time < excluded.navigation_time
                                ;
                                """,
                                record.get("11"),
                                record.get("10"),
                                record.get("14"),
                                record.get("9"),
                                (control_id << 32) | object_id,
                            )

                        await con.execute(
                            """
                                insert into telemetry.location (
                                    terminal_id, 
                                    id, 
                                    navigation_time, 
                                    longitude, 
                                    latitude, 
                                    altitude, 
                                    direction, 
                                    speed,
                                    valid,
                                    sat_count
                                )
                                values (
                                    $1,  
                                    $2,
                                    to_timestamp($3), 
                                    $4 / 600000.0, 
                                    $5 / 600000.0, 
                                    $6 / 10, 
                                    $7, 
                                    $8, 
                                    $9, 
                                    $10 
                                )
                                on conflict do nothing ;
                                """,
                            (control_id << 32) | object_id,
                            record.get("1"),
                            record.get("9"),
                            record.get("11"),
                            record.get("10"),
                            record.get("12"),
                            record.get("14"),
                            record.get("13"),
                            valid,
                            satcount,
                        )
            except Exception as e:
                logger.error(e)
