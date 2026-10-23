from server.user import User


def catalogs(user: User) -> list[str]:
    return sorted(c.name for c in user.client.catalogs.list() if c.name)


def schemas(user: User, catalog: str) -> list[str]:
    return sorted(
        s.name for s in user.client.schemas.list(catalog_name=catalog) if s.name and s.name != "information_schema"
    )
