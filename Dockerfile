FROM python:3.11-slim
WORKDIR /app
# Dependencies first, in a layer that changes only with pyproject.toml, so a
# code change does not download them again.
COPY pyproject.toml ./
RUN pip install --no-cache-dir hatchling hatch-fancy-pypi-readme \
    && mkdir -p src/erpdoo && touch src/erpdoo/__init__.py README.md \
    && pip install --no-cache-dir '.[viewer]' && pip uninstall -y -q erpdoo && rm -rf src README.md
COPY README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps --no-build-isolation . && mkdir -p /var/lib/erpdoo && chown nobody /var/lib/erpdoo
COPY rules ./rules
COPY erpdoo.yml ./
ENV ERPDOO_RULES=/app/rules ERPDOO_CONFIG=/app/erpdoo.yml ERPDOO_STATE=/var/lib/erpdoo/state.db
USER nobody
CMD ["erpdoo"]
