from libprobe.probe import Probe
from lib.check.disk import check_disk
from lib.check.installed_software import check_installed_software
from lib.check.memory import check_memory
from lib.check.network import check_network
from lib.check.nt_domain import check_nt_domain
from lib.check.nt_eventlog import check_nt_eventlog
from lib.check.process import check_process
from lib.check.services import check_services
from lib.check.system import check_system
from lib.check.updates import check_updates
from lib.check.users import check_remote_users
from lib.check.volume import check_volume
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'disk': check_disk,
        'installedSoftware': check_installed_software,
        'memory': check_memory,
        'network': check_network,
        'ntDomain': check_nt_domain,
        'ntEventlog': check_nt_eventlog,
        'process': check_process,
        'services': check_services,
        'system': check_system,
        'updates': check_updates,
        'users': check_remote_users,
        'volume': check_volume,
    }

    probe = Probe("wmi", version, checks)

    probe.start()
