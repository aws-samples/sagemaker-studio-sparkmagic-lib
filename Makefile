.PHONY: clean artifacts release link install test run

release: install test
	make artifacts

install: clean
	pip install -e ".[dev]"

clean:
	rm -rf build/dist

artifacts: clean
	python setup.py sdist --dist-dir build/dist

test:
	pytest -vv .
	black --check .
