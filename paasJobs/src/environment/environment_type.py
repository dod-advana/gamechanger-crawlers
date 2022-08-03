class EnvironmentType:
    """Enumeration of environment type values.

    Attributes:
    
        PRODUCTION (str): Value of the environment when it's in production.

        DEVELOPMENT (str): Value of the environment when it's in dev.
    """

    PRODUCTION = "prod"

    DEVELOPMENT = "dev"

    @staticmethod
    def all():
        """Get all environment types.

        Returns:
            list of str: All valid EnvironmentType values.
        """
        return [EnvironmentType.PRODUCTION, EnvironmentType.DEVELOPMENT]

    @staticmethod
    def is_valid(env_type):
        """Determine whether or not the given env_type is a valid
        EnvironmentType.

        Args:
            env_type (str)

        Returns:
            bool: True if the given `env_type` is a valid EnvironmentType
                (i.e., exists in EnvironmentType.all()), False otherwise.
        """
        return env_type in EnvironmentType.all()
