from __future__ import annotations

import os
from pathlib import Path
import logging
import json
import re

import pendulum
from airflow.sdk import DAG, task, Param, get_current_context
from common.transfer_utils import *

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=15),
    # "execution_timeout": pendulum.duration(hours=6),
}

# Environment Vars
RCLONE_CFG_PATH = os.environ['RCLONE_CONFIG_PATH']


HOST = 'cantaloupe'
HOST_ROOT = '/analysis_raid/merfish_raw_data'
FLAGS_TO_REMOVE = ["MERLIN_START", "MERLIN_FINISHED"]
FINISHED_FLAG = "MERLIN_FINISHED"


logger = logging.getLogger(__name__)

with DAG(
        dag_id="reprocess3dMosaic",
        params={
            "experiment": Param(type="string", title="Name of experiemnt", description="Required. Ex: 202511071124_20251107M251BICANRen64_VMSC32010. Will be igored if Forced path is filled"),
            "force_path": Param(type=["string", 'null'], title="Forced path", description="Direct path to data folder to force reproc without passing checks")
        }
    ) as dag:

    @task
    def check_experiments():
        ctx = get_current_context()
        exp_name = ctx['params']['experiment']
        force_path = ctx['params']['force_path']

        if force_path:
            logger.log(logging.INFO, f"Forcing transfer from: {force_path}")
            return force_path
        logger.log(logging.INFO, exp_name)

        # filt_exp = []
        # for exp in exp_name: # keeping this in case I want to include multiple exp. in the future
        loc = None
        img_folders = list(Path(f"/mnt/").glob(f"merfish1*/MERSCOPE/output/{exp_name}/reg*/images"))
        logger.log(logging.INFO, f"img_folders: {img_folders}")
        if img_folders:
            loc = img_folders.pop()
        else:
            logger.log(logging.ERROR, f"Experiment({exp_name}) was not found")
            raise RuntimeError
        mosaic_paths = list(loc.glob('*mosaic*'))
        z3s = len([p for p in mosaic_paths if 'z3.tif' in str(p)])
        z4s = len([p for p in mosaic_paths if 'z4.tif' in str(p)])
        logger.log(logging.INFO, f"z3#={z3s} z4#={z4s}")
        logger.log(logging.INFO, f"mosaic_paths: {mosaic_paths}")
        if z3s > z4s:
            logger.log(logging.INFO, f"image folder {loc} passes the mosaic check (z3s > z4s)")

            data_path = Path(re.sub('/output/', '/data/', str(loc))).parent.parent
            logger.log(logging.INFO, f"image folder {loc} corresponds to data folder {data_path}")

            if not data_path.exists(): 
                logger.log(logging.ERROR, f"data folder {data_path} does not exist")
                raise RuntimeError()
        else:
            logger.log(logging.ERROR, f"image folder {loc} did not pass the mosaic check (z3s <= z4s)")
            raise RuntimeError

        return str(data_path) # only works on one experiment at a time, this is because we cannot monitor the instruments pipeline easily.


    @task
    def alter_json(data_path):
        
        json_path = Path(data_path) / "experiment.json"

        # NOTE: This modifies the experiment.json before
        with open(json_path, 'r+', encoding='utf-8') as exp_json:
            experiment = json.load(exp_json)

            try:
                # TODO: Look into changing this to avoid wasting time on segmentation
                # experiment['imageProcessingParameters']['segmentationAlgorithm'] = 'no-segmentation'
                experiment['imageProcessingParameters']['mosaicImageOutput'] = "3D"
                experiment['imageProcessingParameters']['vizualizerFileOutput'] = "3D"
            except KeyError as e:
                logger.log(logging.ERROR, "keys not found")
                raise e

            exp_json.seek(0)        # <--- should reset file position to the beginning.
            json.dump(experiment, exp_json, indent=4)
            exp_json.truncate()     # remove remaining part

    @task
    def transfer_data(path):
        import subprocess
        path = Path(path)
        source = path
        dest = f"{HOST}:{HOST_ROOT}/{path.name}/"
        logger.log(logging.INFO, f"source:{source}, dest:{dest}")
        
        raw_cmd = [
            "rclone", "copy",
            str(source),
            str(dest), 
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

        logger.log(logging.INFO, "output")
        # So that we can change which flags we delete easily
        
        return dest

    @task.bash()
    def remove_flags(dest):
        flag_paths = [f'{dest}/{f}' for f in FLAGS_TO_REMOVE]
        delete_cmds = ';'.join([
            f"rclone delete {fp} "
            for fp in flag_paths
        ])
        logger.log(logging.INFO, delete_cmds)
        return "ls" #delete_cmds

    @task
    def wait(dest):
        # Check every hour for MERLIN_FINISHED flag and 15min after.
        import subprocess as sub
        import time

        def check_flags(dest:str):
            output = sub.run((
                "rclone ls "
                f"{dest} " \
                f"--filter '+ {FINISHED_FLAG}' "
                f"--filter '- **' "
                "--max-depth 0 "
                f"--config {RCLONE_CFG_PATH} "
            ),
            capture_output=True, text=True, check=True, shell=True
            )

            return output.stdout.count(FINISHED_FLAG) == 0
        
        while check_flags(dest):
            logger.log(logging.INFO, f"Waiting for analysis...")
            time.sleep(3600)
            continue
        logger.log(logging.INFO, f"Detected finished files, waiting for safety")
        # time.sleep(3600)
        time.sleep(36)

        
        return dest
    
    @task.bash()
    def transfer_mosaic_imgs(path):
        # Source and dest swapped
        path = Path(path)
        data_path = f"{HOST}:{HOST_ROOT}/{path.name}/"
        source = Path(re.sub('/merfish_raw_data/', '/merfish_output/', str(data_path)))
        dest = Path(re.sub('/data/', '/output/', str(path)))

        logger.log(logging.INFO, f"source:{source}, dest:{dest}")

        raw_cmd = [
            "rclone", "copy",
            f"{source}", 
            f"{dest}",
            "--transfers", "4",
            "--checkers", "8",
            "--config", RCLONE_CFG_PATH
        ]
        cmd = " ".join(raw_cmd)
        logger.log(logging.INFO, cmd)

        return "ls" #cmd

    @task.bash
    def write_flags(dest):
        import datetime
        cmd = (
            f"echo '{datetime.datetime.now()}' | "
            f"rclone rcat {dest + '/mosaic_reanalysis_FINISHED'} "
            f"--config {RCLONE_CFG_PATH}"
        )
        logger.log(logging.INFO, cmd)
        return "ls" #cmd 

    path = check_experiments()
    _altered_json = alter_json(path)
    dest = transfer_data(path)
    _removed_flags = remove_flags(dest)
    _waited = wait(dest)
    _transfered = transfer_mosaic_imgs(path)
    _written_flags = write_flags(dest)

    _altered_json >> dest
    _removed_flags >> _waited >> _transfered >> _written_flags
