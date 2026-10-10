"""Real notification channels -- genuine protocols, zero paid external
accounts needed for local development and verification:

- **Email**: real SMTP to Mailpit (a local catcher running in this
  project's own docker-compose stack), not a mock. Amazon SES (the
  spec's deployment choice) is a drop-in swap of host/port/credentials
  at deploy time -- `smtplib.SMTP` talks the same protocol either way;
  see DECISIONS.md for why Mailpit, not SES, for this milestone.
- **Push**: the real Web Push protocol (VAPID-signed, `pywebpush`), not
  a stub. It needs a real `PushSubscription` (endpoint + keys) from a
  browser's `pushManager.subscribe()` call -- there's no web app to
  issue one until Milestone 9, so this module's own live verification
  gets a real subscription from a real headless browser page instead of
  waiting (see DECISIONS.md and `scripts/verify_trip_monitoring.py`).
"""
import base64
import smtplib
from email.mime.text import MIMEText
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from py_vapid import Vapid
from pywebpush import WebPushException, webpush

SMTP_HOST = "localhost"
SMTP_PORT = 1025
FROM_ADDRESS = "alerts@layover.app"

VAPID_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "vapid"
VAPID_PRIVATE_KEY_PATH = VAPID_DIR / "private_key.pem"
VAPID_CLAIMS = {"sub": "mailto:alerts@layover.app"}


def ensure_vapid_keys() -> Path:
    """Generates a real VAPID keypair on first use and reuses it after --
    no external account needed; this is a self-signed identity Web Push
    uses to let a browser verify notifications come from the same server
    it subscribed with, not a third-party credential."""
    VAPID_DIR.mkdir(parents=True, exist_ok=True)
    if not VAPID_PRIVATE_KEY_PATH.exists():
        vapid = Vapid()
        # py_vapid 1.9.2's own generate_keys() passes the ec.SECP256R1
        # *class* instead of an instance to cryptography's
        # generate_private_key() -- cryptography 46.0.0 (installed here)
        # rejects that outright with "curve must be an EllipticCurve
        # instance" (confirmed live, not assumed). Worked around by
        # generating the key the same way generate_keys() intends to but
        # with a real curve instance, then assigning it through the
        # `private_key` setter (which derives the public key
        # automatically) rather than waiting on an upstream fix or
        # pinning an old, unmaintained `cryptography`. See DECISIONS.md.
        vapid.private_key = ec.generate_private_key(ec.SECP256R1())
        vapid.save_key(str(VAPID_PRIVATE_KEY_PATH))
    return VAPID_PRIVATE_KEY_PATH


def vapid_public_key_b64url() -> str:
    """The public key a browser's `pushManager.subscribe()` call needs
    (as `applicationServerKey`), in the raw base64url form the Push API
    expects -- not the PEM file itself."""
    vapid = Vapid.from_file(str(ensure_vapid_keys()))
    raw = vapid.public_key.public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def send_email(to_address: str, subject: str, body: str) -> dict:
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = FROM_ADDRESS
    msg["To"] = to_address
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=5) as smtp:
        smtp.send_message(msg)
    return {"status": "sent", "channel": "email"}


def send_push(subscription_info: dict, payload: str) -> dict:
    try:
        webpush(
            subscription_info=subscription_info,
            data=payload,
            vapid_private_key=str(ensure_vapid_keys()),
            vapid_claims=dict(VAPID_CLAIMS),
        )
        return {"status": "sent", "channel": "push"}
    except WebPushException as exc:
        return {"status": "failed", "channel": "push", "error": str(exc)}
