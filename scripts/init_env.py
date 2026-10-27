"""Create local configuration without printing any secret."""

from pathlib import Path
import secrets
import shutil

root = Path(__file__).resolve().parents[1]
env = root / ".env"
if env.exists():
    print(".env уже существует, сохранён без изменений.")
else:
    text = (root / ".env.example").read_text()
    text = text.replace("ADMIN_TOKEN=\n", f"ADMIN_TOKEN={secrets.token_urlsafe(36)}\n")
    text = text.replace("INTERNAL_SECRET=\n", f"INTERNAL_SECRET={secrets.token_urlsafe(36)}\n")
    with env.open("x") as output:
        output.write(text)
    env.chmod(0o600)
    print("Создан .env. Токен входа находится в ADMIN_TOKEN; не публикуйте этот файл.")
product = root / "config/product.json"
if not product.exists():
    shutil.copyfile(root / "config/product.example.json", product)
print("Настройте config/product.json. По умолчанию включён mock: платных звонков нет.")
