# Operations Guide - TBD

## Unorganized

## Dataset guidelines

5. Funnel Rocket **does not support joins** between datasets. De-normalize your datasets to include any relevant fields 
you want to be able to query. **TBD** refer to the product feed example

Maximum of 1,000 files per dataset. **TBD: guidance on file size limit, e.g. 256mb?** (depends on usage)

Ideally, you should set the number of parititions so that resulting files are within 20-100 MB per each. Having many small
files would utilitize a large number of workers for diminishing returns in performance. Having large files would make 
querying slower as each file is processed by a single task, and may cause OutOfMemory crashes if memory is too tight.
**TBD link to Lambda/worker suggested RAM settings**

6. **Support for nested data:** Funnel Rocket currently has only limited support for nested data, unlike Spark DataFrames.

6.1 **TBD** implement and documents: bitset columns for a limited set of values (up to 512, non-repeating)

6.2 **TBD** which operators can work on other string/numric lists out of the box?

### Data Format Best Practices

8. **Use numeric and bool types where appropriate:** TBD explain

9. **Using categorical field types:** String columns are usually much more resource-hungry: they are slower to store,
inflate file sizes, slower to read and slower to query. Whenever a string column in a DataFrame seems to only contain the 
same values repeating over and over again, all out of a small set of distinct values, then casting the column the 
'category' type is highly recommended and much easier than trying to map distinct values to int ordinals by yourself. 
 Casting is easy, e.g. `df['some_string_column'] = df['some_string_column'].astype('category')`. 

Making a string column into categorical does not mean losing functionality. All string operations are still supported 
and in fact execute up to an order of magnitude faster. It's a no-brainer to use for a column of 10,000 values with only 5 
unique values, but also useful if there are 50k distict values in a column of 1 million values.

Note that the 'category' type is purely a Pandas feature. The Parquet format does have a similar 
method of dictionary compression, which is automatic and transparent to client. It is however not a data type, and 
does not automatically make columns be loaded into DataFrames as categorical. However, when DataFrames are saved to
Paruqet files and later loaded back, Pandas takes care to store custom metadata in the Parquet file so that it knows how
to cast columns back to their set type when loaded. To get the benefits of this type you'd need to cast the 
relevant columns and then **save to Parquet using Pandas**, rather than via other tools. 
**TBD** a dataset-level mapping of columns to load as category (or other types)
