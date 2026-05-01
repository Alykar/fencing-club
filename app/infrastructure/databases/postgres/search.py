def build_like_pattern(value: str | None) -> str | None:
    if not value:
        return None

    return "%" + value.replace("%", "\\%").replace("_", "\\_") + "%"
