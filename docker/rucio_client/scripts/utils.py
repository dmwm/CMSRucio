"""
Common utility functions between scripts
"""

RFC_ATTRIBUTE_ORDER = ["CN", "L", "ST", "O", "OU", "C", "STREET", "DC", "UID"]

def get_attribute(attribute: str) -> str:
    """Extracts attribute key"""
    try:
        return attribute[:attribute.index("=")]
    except ValueError:
        # print(f"element lacks attribute, {attribute}, {dn}")
        return ""


def rfc2253dn(legacy_dn: str, verbose: bool = False) -> str:
    """Converts legacy slash DB to comma separated DN"""
    if not legacy_dn.startswith('/'):
        if verbose:
            print(f'DN does not start with /: {legacy_dn}')
        return legacy_dn

    # replace commas with an escape character
    legacy_dn = legacy_dn.replace(',', r'\,')
    parts = legacy_dn.split('/')[1:]

    # parts are reversed. See https://datatracker.ietf.org/doc/html/rfc2253#section-2.3
    elements = parts[::-1]
    attributes = [get_attribute(e) for e in elements]

    # skip any DNs who have an element without an attribute
    if "" in attributes:
        return ""

    if verbose:
        print(f"Attributes: {attributes}")
        print(f"Expected order: {RFC_ATTRIBUTE_ORDER}")

    indexes = []
    for attr in attributes:
        try:
            indexes.append(RFC_ATTRIBUTE_ORDER.index(attr))
        except ValueError:
            # skips any DNs that don't have a attribute in the RFC_ATTRIBUTE_ORDER
            pass

    # sort existing attributes based on expected attribute order
    result = [a[0] for a in sorted(zip(elements, attributes, indexes), key=lambda x: x[2])]

    if verbose:
        print(f"original: {legacy_dn}\nindexes: {indexes}\nconverted: {','.join(result)}")

    return ','.join(result)
