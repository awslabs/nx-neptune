# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""
Tests for the ParameterMapBuilder class in the opencypher_builder module.
"""

from nx_neptune.clients.opencypher_builder import ParameterMapBuilder


class TestParameterMapBuilder:
    """Test cases for the ParameterMapBuilder class."""

    def test_read_single_param(self):
        """Test reading a single parameter."""
        builder = ParameterMapBuilder()
        params = {"name": "John"}

        masked_params = builder.read_map(params)

        # Check masked parameters
        assert masked_params == {"name": "$0"}

        # Check internal parameter values
        param_values = builder._param_values
        assert param_values == {"0": "John"}

        # Check counter was incremented
        assert builder._counter == 1

    def test_read_multiple_params(self):
        """Test reading multiple parameters."""
        builder = ParameterMapBuilder()
        params = {"name": "John", "age": 30, "city": "Seattle"}

        masked_params = builder.read_map(params)

        # Check masked parameters
        assert masked_params == {"name": "$0", "age": "$1", "city": "$2"}

        # Check internal parameter values
        param_values = builder._param_values
        assert param_values == {"0": "John", "1": 30, "2": "Seattle"}

        # Check counter was incremented correctly
        assert builder._counter == 3

    def test_multiple_read_calls(self):
        """Test making multiple read calls."""
        builder = ParameterMapBuilder()

        # First read call
        params1 = {"name": "John"}
        masked_params1 = builder.read_map(params1)
        assert masked_params1 == {"name": "$0"}

        # Second read call
        params2 = {"age": 30}
        masked_params2 = builder.read_map(params2)
        assert masked_params2 == {"age": "$1"}

        # Third read call
        params3 = {"city": "Seattle"}
        masked_params3 = builder.read_map(params3)
        assert masked_params3 == {"city": "$2"}

        # Check all parameter values were accumulated
        param_values = builder.get_param_values()
        assert param_values == {"0": "John", "1": 30, "2": "Seattle"}

        # Check counter was incremented correctly
        assert builder._counter == 3
