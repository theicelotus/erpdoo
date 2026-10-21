# A model that sets its own _table, for scripts/custom_table_test.py.
{
    "name": "erpdoo test: custom table name",
    "version": "18.0.1.0.0",
    "summary": "Test module for erpdoo; not for production",
    "depends": ["base"],
    "data": ["security/ir.model.access.csv"],
    "license": "Other OSI approved licence",  # MIT, like erpdoo
    "installable": True,
}
