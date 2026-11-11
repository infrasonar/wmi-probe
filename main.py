import resource
import logging
from libprobe.probe import Probe
from lib.check.memory import CheckMemory
from lib.check.network import CheckNetwork
from lib.check.netstat import CheckNetstat
from lib.check.nt_domain import CheckNtDomain
from lib.check.process import CheckProcess
from lib.check.services import CheckServices
from lib.check.software import CheckSoftware
from lib.check.storage import CheckStorage
from lib.check.system import CheckSystem
from lib.check.updates import CheckUpdates
from lib.check.users import CheckUsers
from lib.version import __version__ as version


def raise_file_limit():
    try:
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        logging.info(
            f"Updated file descriptor limit: soft={new_soft}, hard={new_hard}")
    except Exception as e:
        logging.warning(f"Could not raise file descriptor limit: {e}")


if __name__ == '__main__':
    checks = (
        CheckMemory,
        CheckNetwork,
        CheckNetstat,
        CheckNtDomain,
        CheckProcess,
        CheckServices,
        CheckSoftware,
        CheckStorage,
        CheckSystem,
        CheckUpdates,
        CheckUsers,
    )

    probe = Probe("wmi", version, checks, loggers=('aiowmi',))
    raise_file_limit()
    probe.start()
