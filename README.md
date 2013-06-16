database-exporter
=================

Features:

    1.supports export database from each other.
    
    2.supported database: Oracle, MySQL.
    
    3.supports multi source tables merged into one target table.
    
    4.supports one source table split into multi target tables.
    
    5.supports source table column name different from target table column name.
    
    6.supports multi tables exported concurrently.
    
    7.supports one table's data exported concurrently by page.
    

Build:
    
    1. fetch the source code: git clone git://github.com/wucharles/database-exporter.git
    
    2. cd database-exporter
    
    3. mvn package
    
    4.use 'db-export.zip' to export database data

Usage:

    1. In Linux: cd bin ; ./export.sh
    
    2. In Windows: cd bin & export.bat

    Note: Before execute the shell script, install Java runtime in you PC.

Configure:

    1.
