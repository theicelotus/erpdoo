FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && mkdir -p /var/lib/erpdoo && chown nobody /var/lib/erpdoo
COPY rules ./rules
ENV ERPDOO_RULES=/app/rules ERPDOO_STATE=/var/lib/erpdoo/state.db
USER nobody
CMD ["erpdoo"]
