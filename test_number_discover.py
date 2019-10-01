
"""
Tests for NumberDiscover
"""

from whatismyschema import NumberDiscover


class TestNumberDiscover:

    def test_tiny_int(self):
        values = ['0', '1', '2', '127', '-127']
        assert NumberDiscover.discover(values) == 'tinyint'

    def test_small_int(self):
        values = [
            '-1', '0', '1',
            '-32767', '32767'
        ]
        assert NumberDiscover.discover(values) == 'smallint'

    def test_int(self):
        values = [
            '-1', '0', '1',
            '-2147483647', '-2147483647'
        ]
        assert NumberDiscover.discover(values) == 'int'

    def test_big_int(self):
        values = [
            '-1', '0', '1',
            '-2147483647', '-2147483647'
        ]
        assert NumberDiscover.discover(values) == 'int'

    def test_string(self):
        values = [
            '-1', '0', '1',
            '*1'
        ]
        assert NumberDiscover.discover(values) == 'string'

    def test_scientific_notation(self):
        values = [
            '-1', '0', '1',
            '5e-4'
        ]
        assert NumberDiscover.discover(values) == 'decimal'

    def test_implicit_pre_dot(self):
        values = [
            '-1', '0', '1',
            '.01'
        ]
        assert NumberDiscover.discover(values) == 'decimal(3, 2)'

    def test_implicit_post_dot(self):
        values = [
            '-1', '0', '1',
            '1.'
        ]
        assert NumberDiscover.discover(values) == 'tinyint'

    def test_decimal(self):
        values = [
            '-1', '0', '1',
            '1.01'
        ]
        assert NumberDiscover.discover(values) == 'decimal(3, 2)'
