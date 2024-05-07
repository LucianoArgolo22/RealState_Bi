import logging

class ClientLogger:
    """
    A logging utility for a client application.

    Attributes
    ----------
    filename : str
        The filename of the log file.
    level : int
        The logging level (default is logging.INFO).
    format : str
        The format of the log messages (default is '[%(asctime)s] - %(process)d - %(levelname)s - %(message)s').
    app_name : str
        The name of the client application.

    """
    def __init__(self, filename:str, app_name:str) -> None:
        """
        Initialize a new `ClientLogger` instance.

        Parameters
        ----------
        filename : str
            The filename of the log file.
        app_name : str
            The name of the client application.

        """
        self.filename = filename
        self.level = logging.INFO
        self.format = f'[%(asctime)s] - %(process)d - %(levelname)s - %(message)s'
        self.app_name = app_name

    def config(self) -> None:
        """
        Configures the logger with the specified logging level, log message format, and handlers.
        """
        logging.basicConfig(level=self.level, format=self.format,
                            handlers=[logging.FileHandler(self.filename),
                                      logging.StreamHandler()])

    def get_log(self) -> logging.Logger:
        """
        Returns a logger instance with the specified name and configuration.
        """
        self.config()
        return logging.getLogger(self.app_name)

