import ifcopenshell
import ifcopenshell.util.element as util
import math
from neo4j import GraphDatabase

# --- CONFIG ---
NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Amir44813141-"  # the password you set for NexusDatabase
IFC_PATH       = "C:\\Nextcloud\\Promotion\\NexusDatabase\\sample_data\\bimnexus.ifc"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ── IFC PARSER (your original code) ───────────────────────────

def detect_wall_type(wall_name):
    name = (wall_name or "").lower()
    if "ceramic" in name or "tile" in name:   return "ceramic"
    elif "glas"  in name or "glass" in name:  return "glass"
    elif "paint" in name or "farbe" in name:  return "paint"
    elif "gk"    in name or "gips"  in name:  return "drywall"
    elif "beton" in name or "concrete" in name: return "structural"
    else:                                       return "general"

def parse_ifc(file_path):
    model = ifcopenshell.open(file_path)
    floors, spaces, walls, doors, windows, furniture = [], [], [], [], [], []
    boundaries, door_bounds, window_bounds = [], [], []

    for f in model.by_type("IfcBuildingStorey"):
        floors.append({"guid": f.GlobalId, "name": f.Name, "level": f.Elevation or 0})

    for s in model.by_type("IfcSpace"):
        psets = util.get_psets(s)
        area = height = None
        for p in psets.values():
            if "NetFloorArea"        in p: area   = round(p["NetFloorArea"], 2)
            if "FinishCeilingHeight" in p: height = p["FinishCeilingHeight"]
            if "Height"              in p: height = p["Height"]
        spaces.append({"guid": s.GlobalId, "name": s.Name,
                        "long_name": s.LongName or s.Name, "area": area, "height": height})

    seen = set()
    for w in model.by_type("IfcWallStandardCase"):
        if w.GlobalId in seen: continue
        seen.add(w.GlobalId)
        psets = util.get_psets(w)
        is_ext = False
        for p in psets.values():
            if "IsExternal" in p: is_ext = p["IsExternal"]
        mat = util.get_material(w)
        material = None
        if mat:
            if hasattr(mat, "Name"): material = mat.Name
            elif hasattr(mat, "ForLayerSet"):
                material = " | ".join([f"{l.Material.Name}:{round(l.LayerThickness,1)}mm"
                                       for l in mat.ForLayerSet.MaterialLayers if l.Material])
        walls.append({"guid": w.GlobalId, "name": w.Name,
                       "is_external": is_ext, "material": material,
                       "wall_type": detect_wall_type(w.Name)})

    for d in model.by_type("IfcDoor"):
        w, h = d.OverallWidth or 0, d.OverallHeight or 0
        doors.append({"guid": d.GlobalId, "name": d.Name, "width": w, "height": h,
                       "area": round((w*h)/1e6, 2) if w and h else 0})

    for w in model.by_type("IfcWindow"):
        ww, wh = w.OverallWidth or 0, w.OverallHeight or 0
        windows.append({"guid": w.GlobalId, "name": w.Name, "width": ww, "height": wh,
                         "area": round((ww*wh)/1e6, 2) if ww and wh else 0})

    for item in model.by_type("IfcFurnishingElement"):
        space_name = None
        for rel in model.by_type("IfcRelContainedInSpatialStructure"):
            if item in rel.RelatedElements and rel.RelatingStructure.is_a("IfcSpace"):
                space_name = rel.RelatingStructure.LongName or rel.RelatingStructure.Name
        furniture.append({"guid": item.GlobalId, "name": item.Name, "space_name": space_name})

    seen2 = set()
    for b in model.by_type("IfcRelSpaceBoundary"):
        elem, space = b.RelatedBuildingElement, b.RelatingSpace
        if not elem or not space: continue
        key = (elem.GlobalId, space.Name)
        if key in seen2: continue
        seen2.add(key)

        if elem.is_a("IfcWallStandardCase"):
            area = length = height = None
            try:
                surface = b.ConnectionGeometry.SurfaceOnRelatingElement
                height  = round(surface.Depth, 2)
                pts     = surface.SweptCurve.Curve.Points
                p1, p2  = pts[0].Coordinates, pts[1].Coordinates
                length  = round(math.sqrt((p2[0]-p1[0])**2+(p2[1]-p1[1])**2), 2)
                area    = round(height * length, 2)
            except: pass
            boundaries.append({"wall_guid": elem.GlobalId, "wall_name": elem.Name,
                                "wall_type": detect_wall_type(elem.Name),
                                "space_name": space.Name, "space_long": space.LongName or space.Name,
                                "area": area, "length": length, "height": height})

        elif elem.is_a("IfcDoor"):
            w, h = elem.OverallWidth or 0, elem.OverallHeight or 0
            door_bounds.append({"door_guid": elem.GlobalId, "door_name": elem.Name,
                                 "space_name": space.Name, "space_long": space.LongName or space.Name,
                                 "width": w, "height": h, "area": round((w*h)/1e6,2) if w and h else 0})

        elif elem.is_a("IfcWindow"):
            w, h = elem.OverallWidth or 0, elem.OverallHeight or 0
            window_bounds.append({"window_guid": elem.GlobalId, "window_name": elem.Name,
                                   "space_name": space.Name, "space_long": space.LongName or space.Name,
                                   "width": w, "height": h, "area": round((w*h)/1e6,2) if w and h else 0})

    print(f"✅ {len(floors)} floors | {len(spaces)} spaces | {len(walls)} walls | "
          f"{len(doors)} doors | {len(windows)} windows | {len(furniture)} furniture")
    return {"floors": floors, "spaces": spaces, "walls": walls, "doors": doors,
            "windows": windows, "furniture": furniture, "boundaries": boundaries,
            "door_boundaries": door_bounds, "window_boundaries": window_bounds}

# ── NEO4J UPLOAD ───────────────────────────────────────────────

def upload(tx, query, items):
    for item in items:
        tx.run(query, **item)

def main():
    print("📂 Parsing IFC...")
    data = parse_ifc(IFC_PATH)

    with driver.session() as s:
        print("⬆️  Uploading nodes...")
        s.execute_write(upload, "MERGE (n:Floor {guid:$guid}) SET n.name=$name, n.level=$level", data["floors"])
        s.execute_write(upload, "MERGE (n:Space {guid:$guid}) SET n.name=$name, n.long_name=$long_name, n.area=$area, n.height=$height", data["spaces"])
        s.execute_write(upload, "MERGE (n:Wall  {guid:$guid}) SET n.name=$name, n.is_external=$is_external, n.material=$material, n.wall_type=$wall_type", data["walls"])
        s.execute_write(upload, "MERGE (n:Door  {guid:$guid}) SET n.name=$name, n.width=$width, n.height=$height, n.area=$area", data["doors"])
        s.execute_write(upload, "MERGE (n:Window{guid:$guid}) SET n.name=$name, n.width=$width, n.height=$height, n.area=$area", data["windows"])
        s.execute_write(upload, "MERGE (n:Furniture{guid:$guid}) SET n.name=$name, n.space_name=$space_name", data["furniture"])

        print("🔗 Creating relationships...")
        s.execute_write(upload, "MATCH (w:Wall{guid:$wall_guid}) MATCH (s:Space{name:$space_name}) MERGE (w)-[r:BOUNDS]->(s) SET r.area=$area, r.length=$length, r.height=$height", data["boundaries"])
        s.execute_write(upload, "MATCH (d:Door{guid:$door_guid}) MATCH (s:Space{name:$space_name}) MERGE (d)-[r:OPENS_INTO]->(s) SET r.area=$area", data["door_boundaries"])
        s.execute_write(upload, "MATCH (w:Window{guid:$window_guid}) MATCH (s:Space{name:$space_name}) MERGE (w)-[r:FACES]->(s) SET r.area=$area", data["window_boundaries"])
        for f in data["furniture"]:
            if f["space_name"]:
                s.execute_write(lambda tx, f=f: tx.run("MATCH (i:Furniture{guid:$guid}) MATCH (s:Space{name:$space_name}) MERGE (i)-[:LOCATED_IN]->(s)", **f))

    driver.close()
    print("🎉 Done! Open Neo4j Explore to see your graph.")

if __name__ == "__main__":
    main()