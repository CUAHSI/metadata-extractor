.DEFAULT_GOAL := all
isort = python3 -m isort hsextract
black = python3 -m black -S -l 120 --target-version py38 hsextract

.PHONY: format
format:
	$(isort)
	$(black)
