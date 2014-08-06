python-vector-reader
====================

#### add client for vector service
> Base on Redis-py, add vector command.

> transfer firehose data to vector data.

Vector Command
------------------

1. vadd  /* command for add data to vector service. */
2. vrem  /* command for del data to vector service. */
3. vremrange /* command for delete data with range. */
4. vcard /* commnd for query vector count */
5. vcount /* command for query vector count with range */
6. vrange /* comand for query vector data */
7. vmerge /* command for merge vector */

Config Comand 
-------------------

1. config schema /* operate schema or filter for vector. */ 
2. config column /* operate column for schema.  */
