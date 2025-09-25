import asyncio
import asyncpg
import json
import logging
logger = logging.Logger('db')
import pprint

class Writer:
    runner: asyncio.Task

    def __init__(self, queue: asyncio.Queue, db: str):
        self.queue = queue
        self.db = db

    async def start(self):
        self.pool = await asyncpg.create_pool(self.db, min_size=1, max_size=2)
        self.runner = asyncio.create_task(self.run())
        def run_lost(t: asyncio.Task):
            logger.error('db loop broken')
            if e:=t.exception():
                logger.critical(e)
                asyncio.create_task(
                    self.pool.close()
                ).add_done_callback(
                    lambda t: 
                        asyncio.create_task(self.start())
                )
        self.runner.add_done_callback( run_lost )

    async def run(self):
        while True:

            try:
                object_id, control_id, imei, records, evtype = await self.queue.get()
                # logger.debug(pprint.pformat([object_id, control_id, imei, len(records), evtype]))
                async with self.pool.acquire() as con:
                    con: asyncpg.Connection
                    for record in records:
                        await con.execute(
                            "INSERT INTO telemetry.data(object_id, control_id, terminal_ident, data) values ($1,$2,$3,$4)",
                            object_id,
                            control_id,
                            imei,
                            json.dumps(record),
                        )
                        valid = "8" in record and ((record["8"] & 3) == 3)
                        satcount = record.get("8",0) >> 3
                        
                        if record.get("12",0) > 3000 or record.get("12",0) < 0:
                            valid = False

                        await con.execute(
                            """
                                insert into telemetry.location (
                                    object_id, 
                                    control_id, 
                                    terminal_ident, 
                                    navigation_time, 
                                    terminal_time, 
                                    longitude, 
                                    latitude, 
                                    altitude, 
                                    direction, 
                                    speed,
                                    valid,
                                    sat_count
                                    )
                                values ($1, $2, $3, to_timestamp($4), to_timestamp($5), $6 / 600000.0, $7/ 600000.0, $8/10, $9, $10, $11, $12);
                                """,
                            object_id,
                            control_id,
                            imei,
                            record.get("9"),
                            record.get("3"),
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
