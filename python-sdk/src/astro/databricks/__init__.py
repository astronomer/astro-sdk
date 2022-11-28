from sqlalchemy.dialects import registry

registry.register("databricks.connector", "astro.databricks.dialect", "DatabricksDialect")
