from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field


class Product(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=120)
    call_goal: str = Field(min_length=10, max_length=2000)
    facts: list[str] = Field(default_factory=list, max_length=40)
    ready_for_live: bool = False

    def variables(self, contact_name: str) -> dict[str, str]:
        return {
            "product_name": self.name,
            "product_facts": "\n".join(self.facts)[:12000]
            or "Факты не предоставлены. Не придумывай характеристики, цены или условия.",
            "call_goal": self.call_goal,
            "contact_name": contact_name or "собеседник",
        }


def load_product(path: str, live: bool = False) -> Product:
    content = Path(path).read_text(encoding="utf-8")
    if len(content.encode()) > 24000:
        raise ValueError("Конфигурация продукта слишком большая")
    product = Product.model_validate_json(content)
    if any(len(fact) > 1000 for fact in product.facts):
        raise ValueError("Один факт о продукте не должен превышать 1000 символов")
    if live and (not product.ready_for_live or not product.facts):
        raise ValueError("Заполните факты продукта и установите ready_for_live=true перед реальными звонками")
    return product
