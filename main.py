from libprobe.probe import Probe
from lib.check.memory import check_memory
from lib.check.netstat import check_netstat
from lib.check.network import check_network
from lib.check.nt_domain import check_nt_domain
from lib.check.process import check_process
from lib.check.scheduled_jobs import check_scheduled_jobs
from lib.check.services import check_services
from lib.check.software import check_software
from lib.check.storage import check_storage
from lib.check.system import check_system
from lib.check.updates import check_updates
from lib.check.users import check_users
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'memory': check_memory,
        'netstat': check_netstat,
        'network': check_network,
        'ntDomain': check_nt_domain,
        'process': check_process,
        'services': check_services,
        'software': check_software,
        'scheduledJobs': check_scheduled_jobs,
        'storage': check_storage,
        'system': check_system,
        'updates': check_updates,
        'users': check_users,
    }

    probe = Probe("wmi", version, checks)

    probe.start()
