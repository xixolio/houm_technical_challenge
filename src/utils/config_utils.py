import yaml


def read_yaml(file_path):
    """
    Used to read the config file

    Parameters
    ----------
    :param str file_path: name of the file
    :return: dictionary with the yaml info
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)
