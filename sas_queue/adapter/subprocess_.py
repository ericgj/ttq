import logging
import subprocess

logger = logging.getLogger(__name__)

from ..model.command import Command


def run(cmd: Command) -> subprocess.CompletedProcess:
    logger.debug(
        f"""Running command:
  cmd   = {cmd.command}
  cwd   = {cmd.cwd}
  shell = {cmd.shell}
    """
    )
    ret = subprocess.run(
        cmd.command,
        check=True,
        cwd=cmd.cwd,
        shell=cmd.shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    logger.info(
        f"""Ran command successfully:
  cmd   = {cmd.command}
  cwd   = {cmd.cwd}
  shell = {cmd.shell}
  rc    = {ret.returncode}
    """
    )
    return ret
