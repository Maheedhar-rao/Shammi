import sys
from pprint import pprint

from pypdf import PdfReader

def inspect_pdf(path: str):
    print(f"=== Inspecting: {path} ===")
    reader = PdfReader(path)

    # -------- Layer 1: Info metadata --------
    print("\n[Layer 1] Info dictionary:")
    info = reader.metadata or {}
    for k, v in info.items():
        print(f"  {k}: {v}")

    # -------- Layer 2: XMP metadata --------
    print("\n[Layer 2] XMP metadata:")
    xmp = reader.xmp_metadata
    if not xmp:
        print("  (no XMP metadata)")
    else:
        # These attribute names may differ slightly depending on the XMP helper you're using
        try:
            print("  dc:identifier:", getattr(xmp, "dc_identifier", None))
        except Exception as e:
            print("  dc:identifier: error reading:", e)
        try:
            print("  dc:creator:", getattr(xmp, "dc_creator", None))
        except Exception as e:
            print("  dc:creator: error reading:", e)
        try:
            print("  dc:rights:", getattr(xmp, "dc_rights", None))
        except Exception as e:
            print("  dc:rights: error reading:", e)
        try:
            print("  pdf:Keywords:", getattr(xmp, "pdf_keywords", None))
        except Exception as e:
            print("  pdf:Keywords: error reading:", e)

    # -------- Layer 3: Custom PathwayCatalyst.* --------
    print("\n[Layer 3] Custom PathwayCatalyst.* properties:")
    root = reader.trailer["/Root"]
    pc = root.get("/PathwayCatalyst")
    if not pc:
        print("  (no /PathwayCatalyst entry on catalog)")
    else:
        for k, v in pc.items():
            print(f"  {k}: {v}")

    # -------- Layer 4: Trailer /ID --------
    print("\n[Layer 4] Trailer ID:")
    trailer = reader.trailer
    doc_id = trailer.get("/ID")
    print("  /ID:", doc_id)

    # -------- Layer 5: Invisible annotation (first page) --------
    print("\n[Layer 5] First-page annotations:")
    try:
        page0 = reader.pages[0]
        annots = page0.get("/Annots")
        if not annots:
            print("  (no annotations on first page)")
        else:
            print(f"  Found {len(annots)} annotation(s):")
            for annot_ref in annots:
                annot = annot_ref.get_object()
                subtype = annot.get("/Subtype")
                contents = annot.get("/Contents")
                a = annot.get("/A")
                uri = a.get("/URI") if a else None
                print(f"    /Subtype: {subtype}, /Contents: {contents}, /URI: {uri}")
    except Exception as e:
        print("  Error reading annotations:", e)

    # -------- Layer 6: Embedded original PDF --------
    print("\n[Layer 6] Embedded original:")
    names = root.get("/Names")
    if not names:
        print("  (no /Names dictionary on catalog)")
    else:
        embedded_files = names.get("/EmbeddedFiles")
        if not embedded_files:
            print("  (no /EmbeddedFiles in /Names)")
        else:
            print("  /EmbeddedFiles present (raw structure):")
            pprint(embedded_files)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python verify_pdf_fingerprint.py path/to/file.pdf")
        sys.exit(1)
    inspect_pdf(sys.argv[1])
