from cryptography.fernet import Fernet, InvalidToken
import os
import base64
import hashlib

class EncryptionService:
    def __init__(self):
        raw_key = os.getenv('ENCRYPTION_MASTER_KEY')
        if not raw_key:
            raise RuntimeError("ENCRYPTION_MASTER_KEY must be set in environment.")
        # Accept either a raw Fernet key (44 chars) or any string (derive via SHA-256)
        try:
            decoded = base64.urlsafe_b64decode(raw_key + '==')
            if len(decoded) != 32:
                raise ValueError
            self._fernet = Fernet(raw_key.encode())
        except Exception:
            # Derive a proper 32-byte key from whatever string was provided
            derived = hashlib.sha256(raw_key.encode()).digest()
            fernet_key = base64.urlsafe_b64encode(derived)
            self._fernet = Fernet(fernet_key)

    def encrypt(self, plaintext: str) -> str:
        return self._fernet.encrypt(plaintext.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        try:
            return self._fernet.decrypt(ciphertext.encode()).decode()
        except InvalidToken:
            raise ValueError("Decryption failed: token is invalid or key has changed.")

    def mask_for_display(self, plaintext: str) -> str:
        """Return masked version for UI display: gsk_****...****"""
        if len(plaintext) <= 8:
            return "****"
        return plaintext[:4] + "****" + plaintext[-4:]

# Singleton
encryption_service = EncryptionService()
