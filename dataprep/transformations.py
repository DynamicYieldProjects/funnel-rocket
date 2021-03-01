import math

'''
Pandas does not natively support nested data or lists,
however: if the list is actually a set of values from a small list of possible values,
we can efficiently encode set membership (whether possible value X appears or not) using bit flags.
We're using one or more 64-bit integers per each such set, with a defined maximum number of integers
(so if the list of possible values gets too long - we'll know)

Example: for each user activity, there's a list of audience segment IDs the user was member of 
(at the activity time). If we know that a customer account is limited to, say, 100 audiences - 
then we know the limit on possible IDs (in a single file!)

'''
# TODO: write the mapping to custom metadata in the output Parquet file for the query executor code


INT64_BITS = 64
MAX_BIT_FIELDS_PER_SET = 8


# Create mapping dict of original ID => field number, bit number
def create_bit_field_mapping(all_original_ids):
    if len(all_original_ids) / INT64_BITS > MAX_BIT_FIELDS_PER_SET:
        raise Exception("Original set too large")

    mapping = dict()
    for running_id, original_id in enumerate(all_original_ids):
        field_number = math.floor(running_id / INT64_BITS)
        bit_number = running_id % INT64_BITS  # 0..63
        mapping[original_id] = {'running_id': running_id,
                                'field_number': field_number,
                                'bit_number': bit_number}
    return mapping


# Convert a given set of IDs to a dict of bit-field name => field value with relevant bits on,
# using the given mapping.
# field_name_prefix determines the name of the fields, such as "audiences0", "audiences1", ...
def to_bit_fields(original_ids, mapping, field_name_prefix):
    if len(original_ids) == 0:
        return None

    # Original ID => tuple of (field number, bit number)
    map_entries = [mapping[orig_id] for orig_id in original_ids]
    mapped_ids = [(map_info['field_number'], map_info['bit_number']) for map_info in map_entries]

    # Over an empty array of all fields, turn on the right bits
    fields_array = [0] * MAX_BIT_FIELDS_PER_SET
    for field_number, bit_number in mapped_ids:
        fields_array[field_number] |= (2 ** bit_number)

    # Return a dict of full field name => value, just for fields that have any bit on
    fields_dict = dict()
    for field_number, field_value in enumerate(fields_array):
        if field_value != 0:
            field_name = "{}{}".format(field_name_prefix, field_number)
            fields_dict[field_name] = field_value

    return fields_dict
