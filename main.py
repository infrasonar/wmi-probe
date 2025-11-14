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

    probe = Probe("wmi", version, checks)
    probe.start()
