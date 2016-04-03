all: help

help:
	@echo "Usage: make test -- Runs tests."

clean:
	@echo "Cleaning up build and *.pyc files..."
	@find . -name '*.pyc' -delete
	@rm -rf .coverage
	@rm -rf ./build
	@rm -rf ./dist
	@rm -rf ./MANIFEST
	@echo "Done!"

test: clean unit

unit:
	@coverage run --branch `which nosetests` -vv --with-yanc -s tests/
	@coverage report -m --fail-under=80

coverage-html: unit
	@coverage html -d cover

tox:
	@tox
