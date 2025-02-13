<p align="center">
    <img src="https://raw.githubusercontent.com/Ascentrade/docs/main/assets/icon_plain.svg" alt="Ascentrade Logo"/>
</p>

# Ascentrade Updater

This repository provides the data crawler and updater scripts.

## Virtual Environment
Create a virtual Python 3.10 environment and activate + install all dependencies.
```
python3.10 -m venv venv
source venv/bin/activate
pip3.10 install -r requirements.txt
```
Unless the **ascentrade_client** libraray is available from public PyPI you need to install it from your local build.
Make sure to change the path to the *.tar.gz package if your **interface** folder is not in the parent directory.
```
pip install ../interface/dist/ascentrade_client-*.tar.gz
```

## Contributing

We encourage public contributions! Please review [CONTRIBUTING.md](https://github.com/Ascentrade/docs/blob/main/CONTRIBUTING.md) for details.

## Unit Tests
Run ```python test.py``` from inside this repository to execute the unit test script.
```
test_splitError (__main__.TestSplitParse) ... ok
test_valid1 (__main__.TestSplitParse) ... ok
test_valid2 (__main__.TestSplitParse) ... ok

----------------------------------------------------------------------
Ran 3 tests in 0.000s

OK
```

## License

<p align="center">
    <img src="https://www.gnu.org/graphics/agplv3-with-text-162x68.png" alt="GNU Affero General Public License Version 3"/>
</p>

```
Copyright (C) 2024 Dennis Greguhn and Pascal Dengler

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
```

See [LICENSE](./LICENSE) for more information.