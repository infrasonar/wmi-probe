import aiodns
import asyncio
import time


_MAX_TTL = 1800.0
_DLOOKUP: dict[str, tuple[str, int, float]] = {}


async def _get_kerberos_kdc(domain: str) -> tuple[str, int]:
    loop = asyncio.get_event_loop()
    resolver = aiodns.DNSResolver(loop=loop)
    target = f"_kerberos._tcp.{domain}"

    try:
        result = await resolver.query(target, 'SRV')
        srv_records = sorted(result, key=lambda r: (r.priority, -r.weight))
        return srv_records[0].host.rstrip('.'), srv_records[0].port
    except aiodns.error.DNSError as e:
        raise aiodns.error.DNSError(
            f"DNS lookup (kdc for domain: {domain}) failed: {e}")


async def get_kdc(domain: str) -> tuple[str, int]:
    domain_caps = domain.upper()
    now = time.time()
    kdc_host, kdc_port, ttl = _DLOOKUP.get(domain_caps, (None, None, 0.0))
    if now > ttl:
        kdc_host, kdc_port = await _get_kerberos_kdc(domain)
        _DLOOKUP[domain_caps] = kdc_host, kdc_port, now + _MAX_TTL
    assert kdc_host and kdc_port
    return kdc_host, kdc_port
