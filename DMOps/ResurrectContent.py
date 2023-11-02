import asyncio
import logging
from rucio.client import Client
import pandas as pd

logging.basicConfig(filename="resurrect_content.log",filemode='a' ,level=logging.INFO, format='%(levelname)s %(asctime)s [%(name)s] %(filename)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

async def worker_resurrect(queue):
    client = Client()

    while True:
        container = await queue.get()
        try:
            client.resurrect([{'scope':'cms','name':container,'type':'CONTAINER'}])
            logging.info("Resurrected container %s" % container)
        except Exception as e:
            logging.error("Error resurrecting container %s: %s" % (container, e))
        queue.task_done()

async def worker_attach_dids(queue):
    client = Client()

    while True:
        record = await queue.get()
        try:
            client.attach_dids(scope='cms', name=record[0], dids=[{'scope': 'cms', 'name':record[1]}])
            logging.info("Attached dataset %s to container %s" % (record[1], record[0]))
        except Exception as e:
            logging.error("Error attaching dataset %s to container %s: %s" % (record[1], record[0], e))
        queue.task_done()

async def main():
    filename = '/eos/user/a/amanriqu/SWAN_projects/data-transfer-team/ghost_cont_datasets_att.csv'
    df = pd.read_csv(filename)

    loop = asyncio.get_event_loop()
    queue_resurrect = asyncio.Queue()
    queue_attach_dids = asyncio.Queue()

    num_workers_resurrect = 10
    num_workers_attach_dids = 20

    # Start  Resurrect workers
    workers_resurrect = [loop.create_task(worker_resurrect(queue_resurrect)) for _ in range(num_workers_resurrect)]
    for container in df['CONTAINER'].unique():
        await queue_resurrect.put(container)
    await queue_resurrect.join()
    for worker_task in workers_resurrect:
        worker_task.cancel()

    # Start  Attach dids workers
    workers_attach_dids = [loop.create_task(worker_attach_dids(queue_attach_dids)) for _ in range(num_workers_attach_dids)]
    for record in df.to_numpy():
        await queue_attach_dids.put(record)
    await queue_attach_dids.join()
    for worker_task in workers_attach_dids:
        worker_task.cancel()

if __name__ == '__main__':
    logging.basicConfig(filename="resurrect_content.log",filemode='a' ,level=logging.INFO, format='%(levelname)s %(asctime)s [%(name)s] %(filename)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())