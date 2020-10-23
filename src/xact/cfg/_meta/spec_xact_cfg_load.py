# -*- coding: utf-8 -*-
"""
Functional specification for the xact.cfg.load module.

"""


import itertools

import xact.util


# =============================================================================
class SpecifyFromPath:
    """
    Spec for the xact.cfg.from_path function.

    """

    # -------------------------------------------------------------------------
    def it_returns_a_python_dict(self, filepath_cfg_yaml):
        """
        Xact.cfg.from_path returns a dict.

        """
        import xact.cfg.load

        assert isinstance(xact.cfg.load.from_path(filepath_cfg_yaml), dict)

    # -------------------------------------------------------------------------
    def it_loads_a_single_yaml_file(self, dict_of_strings, filepath_cfg_yaml):
        """
        Xact.cfg.from_path can load configuration from a single YAML file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_path(filepath_cfg_yaml), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_single_json_file(self, dict_of_strings, filepath_cfg_json):
        """
        Xact.cfg.from_path can load configuration from a single JSON file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_path(filepath_cfg_json), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_single_xml_file(self, dict_of_strings, filepath_cfg_xml):
        """
        Xact.cfg.from_path can load configuration from a single XML file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_path(filepath_cfg_xml), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_directory_of_yaml_files(self, dict_of_strings, dirpath_cfg_yaml):
        """
        Xact.cfg.from_path can load configuration from a single YAML file.

        """
        import xact.cfg.load

        loaded = xact.cfg.load.from_path(dirpath_cfg_yaml)
        check_match(xact.cfg.load.from_path(dirpath_cfg_yaml), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_directory_of_json_files(self, dict_of_strings, dirpath_cfg_json):
        """
        Xact.cfg.from_path can load configuration from a single JSON file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_path(dirpath_cfg_json), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_directory_of_xml_files(self, dict_of_strings, dirpath_cfg_xml):
        """
        Xact.cfg.from_path can load configuration from a single XML file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_path(dirpath_cfg_xml), dict_of_strings)


# =============================================================================
class SpecifyFromFilePath:
    """
    Spec for the xact.cfg.load.from_filepath function.

    """

    # -------------------------------------------------------------------------
    def it_returns_a_python_dict(self, filepath_cfg_yaml):
        """
        Xact.cfg.load.from_filepath returns a dict.

        """
        import xact.cfg.load

        assert isinstance(xact.cfg.load.from_filepath(filepath_cfg_yaml), dict)

    # -------------------------------------------------------------------------
    def it_loads_a_single_yaml_file(self, dict_of_strings, filepath_cfg_yaml):
        """
        Xact.cfg.load.from_filepath can load config. from a single YAML file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_filepath(filepath_cfg_yaml), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_single_json_file(self, dict_of_strings, filepath_cfg_json):
        """
        Xact.cfg.load.from_filepath can load config. from a single JSON file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_filepath(filepath_cfg_json), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_single_xml_file(self, dict_of_strings, filepath_cfg_xml):
        """
        Xact.cfg.load.from_filepath can load config. from a single XML file.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_filepath(filepath_cfg_xml), dict_of_strings)


# =============================================================================
class SpecifyLoad:
    """
    Spec for the xact.cfg.load function.

    """

    # -------------------------------------------------------------------------
    def it_returns_a_python_dict(self, dirpath_cfg_yaml):
        """
        Xact.cfg.load returns a dict.

        """
        import xact.cfg.load

        assert isinstance(xact.cfg.load.from_dirpath(dirpath_cfg_yaml), dict)

    # -------------------------------------------------------------------------
    def it_loads_a_directory_of_yaml_files(self, dict_of_strings, dirpath_cfg_yaml):
        """
        Xact.cfg.load loads a directory of yaml files.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_dirpath(dirpath_cfg_yaml), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_directory_of_json_files(self, dict_of_strings, dirpath_cfg_json):
        """
        Xact.cfg.load loads a directory of json files.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_dirpath(dirpath_cfg_json), dict_of_strings)

    # -------------------------------------------------------------------------
    def it_loads_a_directory_of_xml_files(self, dict_of_strings, dirpath_cfg_xml):
        """
        Xact.cfg.load loads a directory of xml files.

        """
        import xact.cfg.load

        check_match(xact.cfg.load.from_dirpath(dirpath_cfg_xml), dict_of_strings)


# ------------------------------------------------------------------------------
def check_match(loaded, true):
    """
    Yield leaf values taken pairwise from the two specified nested maps.

    """
    for pair in itertools.zip_longest(_iter_leaves(loaded), _iter_leaves(true)):

        (pv_loaded, pv_true) = pair

        (path_loaded, value_loaded) = pv_loaded
        (path_true, value_true) = pv_true

        assert path_loaded == path_true
        assert value_loaded == value_true


# ------------------------------------------------------------------------------
def _iter_leaves(data_structure):
    """
    Yield al the leaf values in the specified data_structure.

    """
    for pv_pair in xact.util.gen_path_value_pairs_depth_first(data_structure):
        (path, value) = pv_pair
        is_interior_node = isinstance(value, dict) or isinstance(value, list)
        is_leaf = not is_interior_node
        if is_leaf:
            yield pv_pair
