from __future__ import annotations

import os
from pathlib import Path
import logging
import re
import datetime

import pendulum
from airflow.sdk import DAG, task
from common.transfer_utils import *

ENV_KEYS = [
    'WORKER_NAME',
    'SOURCE_ROOT', 
    'DEST_REMOTE',
    'DEST_ROOT',
    'RCLONE_CONFIG_PATH',
]

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
    # "execution_timeout": pendulum.duration(hours=6),
}

# Environment Vars
RCLONE_CFG_PATH = os.environ['RCLONE_CONFIG_PATH']


# Literals
DATA_PATHS_LITERAL = "data_paths" 
OUTPUT_PATHS_LITERAL = "output_paths"

logger = logging.getLogger(__name__)


def build_transfer_dag(route, all_cfg:dict): # written this way to turn this into a factory
    
    cfg = all_cfg[route]
    ROUTE_NAME = route
    SOURCE_REMOTE = cfg['source']['remote']
    SOURCE_ROOT = cfg['source']['root']
    DEST_REMOTE = cfg['dest']['remote']
    DEST_ROOT = cfg['dest']['root']

    if SOURCE_REMOTE:
        SOURCE=f"{SOURCE_REMOTE}:{SOURCE_ROOT}"
    else:
        SOURCE=SOURCE_ROOT
    
    if DEST_REMOTE:
        DEST=f"{DEST_REMOTE}:{DEST_ROOT}"
    else:
        DEST=DEST_ROOT

    with DAG(
        dag_id=f"merscopeToNas_{ROUTE_NAME}",
        description="Transfers files from MERSCOPE instrument to NAS storage via rclone",
        default_args=DEFAULT_ARGS,
        schedule="0 2 * * *",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["file-transfer", "merscopeToNas"],
    ) as dag:

        @task(
            task_id="get_finished_experiments",
            queue=f"merscopeToNas_{ROUTE_NAME}"
        )
        def list_experiments_on_source() -> str:
            import subprocess

            raw_cmd = [
                "rclone", "ls",
                SOURCE,
                "--filter", "'+ *data*/'",
                "--filter", "'+ *output*/'",
                "--filter", "'+ *MERLIN_FINISHED'",
                "--filter", "'+ *merscopeToNas_FINISHED'",
                "--filter", "'- **'",
                "--max-depth", "3",
                "--config", RCLONE_CFG_PATH,
            ]
            cmd = " ".join(raw_cmd)
            logger.info(cmd)

            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True, shell=True)
            except subprocess.CalledProcessError as e:
                logger.info("rclone cmd errored out")
                logger.info(e.stderr)
                raise

            logger.info(result.stdout)
            return result.stdout


        @task(
                task_id=f"parse_paths_of_finished_data",
                queue=f"merscopeToNas_{ROUTE_NAME}"
            )
        def parse_experiment_list(rclone_ls_str):
            """Returns a list of pairs of paths to transfer."""
            logger.log(logging.INFO, rclone_ls_str)

            merlin_finished = re.compile(r".*/MERLIN_FINISHED")
            data_transfered = re.compile(r"[a-Z0-9]*data.*/.*/merscopeToNas_FINISHED")
            output_transfered = re.compile(r"[a-Z0-9]*output.*/.*/merscopeToNas_FINISHED")

            rclone_ls_str_list = [s.strip() for s in rclone_ls_str.split("\n") if s.strip()]
            rclone_ls_str_list_clean = [s.split(None, 1)[1] for s in rclone_ls_str_list]
            rclone_ls_str_merfin = list(filter(merlin_finished.match, rclone_ls_str_list_clean))
            rclone_ls_str_data = list(filter(data_transfered.match, rclone_ls_str_list_clean))
            rclone_ls_str_output = list(filter(output_transfered.match, rclone_ls_str_list_clean))

            finished_runs = {Path(s).parent.name for s in rclone_ls_str_merfin}   # .../<run>/MERLIN_FINISHED
            data_done = {Path(s).parts[1] for s in rclone_ls_str_data}            # merfish_raw_data/<run>/...
            output_done = {Path(s).parts[1] for s in rclone_ls_str_output}        # merfish_output/<run>/...

            to_transfer = [
                Path(top) / run
                for run in sorted(finished_runs)
                for top, done in (("merfish_raw_data", data_done), ("merfish_output", output_done))
                if run not in done
            ]

            renames = {"merfish_output": "output", "merfish_raw_data": "data"}
            to_transfer_dest = [
                str(p) + "$" + str(Path(*(renames.get(part, part) for part in p.parts)))
                for p in to_transfer
            ]
            return to_transfer_dest  
        

        @task(task_id="rclone_transfer", queue=f"merscopeToNas_{ROUTE_NAME}")
        def transfer_data(path_pair):
            import subprocess
            source_path, dest_path = path_pair.split('$')
            
            source = f"{SOURCE}/{source_path}"
            dest = f"{DEST}/{dest_path}"
            logger.log(logging.INFO, f"source:{source}, dest:{dest}")

            raw_cmd = [
                "rclone", "copy",
                source,
                dest, 
                "--transfers", "4",
                "--checkers", "8",
                "--config", RCLONE_CFG_PATH
            ]
            cmd = " ".join(raw_cmd)
            logger.log(logging.INFO, cmd)

            try:
                output = subprocess.run(cmd, capture_output=True, text=True, check=True, shell=True)
            except subprocess.CalledProcessError as e:
                logger.log(logging.INFO, e.stderr)
                raise

            logger.log(logging.INFO, output)

            if output.returncode == 0: #redundant whihe check=True above. keep in case subprocess logic changes
                logger.log(logging.INFO, "Adding flag")
                subprocess.run(
                    f"echo '{datetime.datetime.now()}' | rclone rcat {source + '/merscopeToNas_FINISHED'} -- config {RCLONE_CFG_PATH}",
                    capture_output=True, text=True, check=True, shell=True
                )
            else:
                logger.log(logging.INFO, "Not adding flag")
                raise Exception


        # rclone_ls_str >> parsed_list >> transefer_data_task
        rclone_ls_str = list_experiments_on_source()
        parsed_list = parse_experiment_list(rclone_ls_str)
        transefer_data_task = transfer_data.expand(path_pair=parsed_list)

        return dag

all_cfg = parse_source_dest_config()[Path(__file__).stem]

dags = []
for route in all_cfg:
    dags.append(build_transfer_dag(route, all_cfg))

if __name__ == "__main__":
    breakpoint()
    # for dag in dags:
    #     dag.test()

    # set up regex