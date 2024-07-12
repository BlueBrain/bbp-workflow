"""Utils for tests."""


class set_luigi_config:
    """Context manager to set current luigi config.

    Args:
        params (dict): The parameters to load into the luigi configuration.
        configfile (str): Path to the temporary config file (luigi.cfg by default).
    """

    def __init__(self, params=None, configfile=None):
        self.params = params
        self.luigi_config = luigi.configuration.get_config()
        if configfile is not None:
            self.configfile = configfile
        else:
            self.configfile = "luigi.cfg"

    def __enter__(self):
        """Load the given luigi configuration."""
        # Reset luigi config
        self.luigi_config.clear()

        # Remove config file
        if os.path.exists(self.configfile):
            os.remove(self.configfile)

        # Export config
        if self.params is not None:
            self.export_config(self.configfile)

            # Set current config in luigi
            self.luigi_config.read(self.configfile)
        else:
            self.configfile = None

    def get_config(self):
        """Convert the parameter dict to a :class:`configparser.ConfigParser` object."""
        config = ConfigParser()
        config.read_dict(self.params)
        return config

    def export_config(self, filepath):
        """Export the configuration to the configuration file."""
        params = self.get_config()

        # Export params
        with open(filepath, "w") as configfile:  # pylint: disable=unspecified-encoding
            params.write(configfile)

    def __exit__(self, *args):
        """Reset the configuration when exiting the context manager."""
        # Remove config file
        if self.configfile is not None and os.path.exists(self.configfile):
            os.remove(self.configfile)

        # Reset luigi config
        self.luigi_config.clear()
