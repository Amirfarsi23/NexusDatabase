"""
neo4j_handler.py  —  BIM Nexus
────────────────────────────────────────────────────────────────
Fixes applied vs previous version:

  FIX 1 (CRITICAL) — Split wall segments no longer lost
         Old dedup key: (wall_guid, space_name)
         New dedup key: (wall_guid, space_name, start_x_rounded)
         Result: GK 100:2436079 now stores ALL 4 segments

  FIX 2 (CRITICAL) — Segment coordinates extracted
         start_x, start_y, end_x, end_y, segment_index now parsed

  FIX 3 (CRITICAL) — Segment nodes created in Neo4j
         (Wall)-[:HAS_SEGMENT]->(Segment)-[:FACES]->(Space)
         Purpose 1 (QTO):     query Wall.area directly
         Purpose 2 (Raumbuch): query Segment.area per Space

  FIX 4 (CRITICAL) — Wall→Window/Door HOSTS relationship added
         Parsed via Wall.HasOpenings → IfcOpeningElement → HasFillings
         Stored as (Wall)-[:HOSTS]->(Window/Door)

  FIX 5 (BUG) — Door/Window area calculation corrected
         Old: round((w * h) / 1e6, 4)  → ~0.0 m² (wrong, treats m as mm)
         New: round(w * h, 4)           → 2.048 m² (correct, already metres)

  FIX 6 (BUG) — Wall loop 'area' variable now initialized to None
         Prevents bleed-over from previous wall iteration

  FIX 7 — length/height added to boundary records
         Needed for Raumbuch VOB calculations
"""

import math
import json
import os
import ifcopenshell
import ifcopenshell.util.element as util
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
IFC_PATH       = os.getenv("IFC_PATH", "sample_data/bimnexus.ifc")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ── HELPERS ────────────────────────────────────────────────────────────────

def detect_wall_type(wall_name):
    name = (wall_name or "").lower()
    if any(k in name for k in ["ceramic", "fliese", "tile"]):   return "ceramic"
    if any(k in name for k in ["glas", "glass"]):               return "glass"
    if any(k in name for k in ["paint", "farbe"]):              return "paint"
    if any(k in name for k in ["gk", "gips", "trockenbau"]):    return "drywall"
    if any(k in name for k in ["ziegel", "beton", "concrete"]): return "structural"
    return "general"


def get_floor_name(element):
    try:
        container = util.get_container(element)
        if container and container.is_a("IfcBuildingStorey"):
            return container.Name
    except:
        pass
    return None


def get_type_pset_value(element, pset_name, prop_name):
    for definition in element.IsDefinedBy:
        if definition.is_a("IfcRelDefinesByType"):
            element_type = definition.RelatingType
            if not hasattr(element_type, "HasPropertySets") or not element_type.HasPropertySets:
                continue
            for pset in element_type.HasPropertySets:
                if pset.Name == pset_name:
                    for prop in pset.HasProperties:
                        if prop.Name == prop_name:
                            try:    return prop.NominalValue.wrappedValue
                            except: return None
    return None


def get_instance_pset_value(element, pset_name, prop_name):
    psets = util.get_psets(element)
    for name, props in psets.items():
        if name == pset_name and prop_name in props:
            return props[prop_name]
    return None


def get_material_info(element):
    mat             = util.get_material(element)
    material_str    = None
    material_layers = []
    if not mat:
        return None, "[]"
    try:
        if hasattr(mat, "Name") and mat.Name:
            material_str = mat.Name
        elif hasattr(mat, "ForLayerSet"):
            layers = mat.ForLayerSet.MaterialLayers
            parts  = []
            for layer in layers:
                if layer.Material:
                    t = round(layer.LayerThickness * 1000, 1)
                    parts.append(f"{layer.Material.Name}:{t}mm")
                    material_layers.append({"name": layer.Material.Name, "thickness_mm": t})
            material_str = " | ".join(parts)
        elif hasattr(mat, "MaterialConstituents"):
            parts = []
            for c in mat.MaterialConstituents:
                if c.Material:
                    parts.append(c.Material.Name)
                    material_layers.append({"name": c.Material.Name, "thickness_mm": None})
            material_str = " | ".join(parts)
    except:
        pass
    return material_str, json.dumps(material_layers)


def polygon_area(points):
    """Shoelace formula for 2D polygon area."""
    n = len(points)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    return abs(area) / 2.0


def get_boundary_geometry(boundary):
    """
    Returns (area, length, height, start_x, start_y, end_x, end_y).
    Handles both IfcSurfaceOfLinearExtrusion and IfcCurveBoundedPlane.
    """
    try:
        if not boundary.ConnectionGeometry:
            return None, None, None, None, None, None, None
        geom = boundary.ConnectionGeometry.SurfaceOnRelatingElement

        if geom.is_a("IfcSurfaceOfLinearExtrusion"):
            height  = geom.Depth
            profile = geom.SweptCurve
            if not hasattr(profile, "Curve") or not hasattr(profile.Curve, "Points"):
                return None, None, None, None, None, None, None
            pts = [p.Coordinates for p in profile.Curve.Points]
            if len(pts) < 2:
                return None, None, None, None, None, None, None

            # Sum all sub-segments
            length = 0.0
            for i in range(len(pts) - 1):
                p1, p2 = pts[i], pts[i + 1]
                length += math.sqrt((p2[0]-p1[0])**2 + (p2[1]-p1[1])**2)

            area    = round(length * height, 4)
            length  = round(length, 4)
            height  = round(height, 4)
            start_x = round(pts[0][0], 4)
            start_y = round(pts[0][1], 4)
            end_x   = round(pts[-1][0], 4)
            end_y   = round(pts[-1][1], 4)
            return area, length, height, start_x, start_y, end_x, end_y

        elif geom.is_a("IfcCurveBoundedPlane"):
            pts  = [(p.Coordinates[0], p.Coordinates[1])
                    for p in geom.OuterBoundary.Points]
            area = round(polygon_area(pts), 4)
            return area, None, None, None, None, None, None

    except Exception as e:
        pass
    return None, None, None, None, None, None, None


# ── IFC PARSER ─────────────────────────────────────────────────────────────

def parse_ifc(file_path):
    print(f"\n📂 Opening IFC: {file_path}")
    model = ifcopenshell.open(file_path)

    floors        = []
    spaces        = []
    walls         = []
    slabs         = []
    doors         = []
    windows       = []
    furniture     = []
    segments      = []   # ← NEW: wall boundary segments (Raumbuch)
    door_bounds   = []
    window_bounds = []
    slab_bounds   = []
    hosted        = []   # ← NEW: wall → window/door hosting

    # ── Floors ──────────────────────────────────────────────────
    for f in model.by_type("IfcBuildingStorey"):
        floors.append({"guid": f.GlobalId, "name": f.Name, "level": f.Elevation or 0})

    # ── Spaces ──────────────────────────────────────────────────
    for s in model.by_type("IfcSpace"):
        psets  = util.get_psets(s)
        area   = None
        height = None
        for p in psets.values():
            if "NetFloorArea"        in p and p["NetFloorArea"]: area   = round(p["NetFloorArea"], 2)
            if "FinishCeilingHeight" in p: height = p["FinishCeilingHeight"]
            if "Height"              in p: height = p["Height"]
        spaces.append({
            "guid":       s.GlobalId,
            "name":       s.Name,
            "long_name":  s.LongName or s.Name,
            "area":       area,
            "height":     height,
            "usage":      get_instance_pset_value(s, "Pset_SpaceCommon", "OccupancyType"),
            "floor_name": get_floor_name(s),
        })

    # ── Walls ───────────────────────────────────────────────────
    seen_walls = set()
    all_walls  = list(model.by_type("IfcWall")) + list(model.by_type("IfcWallStandardCase"))
    for w in all_walls:
        if w.GlobalId in seen_walls:
            continue
        seen_walls.add(w.GlobalId)

        psets        = util.get_psets(w)
        is_external  = False
        load_bearing = False
        # FIX 6: initialize area to None before loop
        length = height = width = area = None

        for p in psets.values():
            if "IsExternal"    in p: is_external  = p["IsExternal"]
            if "LoadBearing"   in p: load_bearing = p["LoadBearing"]
            if "Length"        in p: length = round(p["Length"],        3)
            if "Height"        in p: height = round(p["Height"],        3)
            if "Width"         in p: width  = round(p["Width"],         3)
            if "NetSideArea"   in p: area   = round(p["NetSideArea"],   3)
            if "GrossSideArea" in p and area is None: area = round(p["GrossSideArea"], 3)
            if "Area"          in p and area is None: area = round(p["Area"],          3)

        if area is None and length and height:
            area = round(length * height, 3)

        material_str, material_layers = get_material_info(w)

        walls.append({
            "guid":            w.GlobalId,
            "name":            w.Name,
            "is_external":     is_external,
            "load_bearing":    load_bearing,
            "u_value":         round(get_type_pset_value(w, "Pset_WallCommon", "ThermalTransmittance"), 4)
                               if get_type_pset_value(w, "Pset_WallCommon", "ThermalTransmittance") else None,
            "fire_rating":     get_type_pset_value(w, "Pset_WallCommon", "FireRating") or
                               get_instance_pset_value(w, "Pset_WallCommon", "FireRating"),
            "material":        material_str,
            "material_layers": material_layers,
            "wall_type":       detect_wall_type(w.Name),
            "length":          length,
            "height":          height,
            "width":           width,
            "floor_name":      get_floor_name(w),
            "area":            area,
        })

    # ── Slabs ───────────────────────────────────────────────────
    seen_slabs = set()
    for sl in model.by_type("IfcSlab"):
        if sl.GlobalId in seen_slabs:
            continue
        seen_slabs.add(sl.GlobalId)
        psets       = util.get_psets(sl)
        is_external = False
        area = thickness = None
        for p in psets.values():
            if "IsExternal" in p: is_external = p["IsExternal"]
            if "NetArea"    in p: area      = round(p["NetArea"],   2)
            if "GrossArea"  in p and area is None: area = round(p["GrossArea"], 2)
            if "Depth"      in p: thickness = round(p["Depth"],     3)
            if "Width"      in p and thickness is None: thickness = round(p["Width"], 3)

        material_str, material_layers = get_material_info(sl)
        slabs.append({
            "guid":            sl.GlobalId,
            "name":            sl.Name,
            "slab_type":       str(sl.PredefinedType) if hasattr(sl, "PredefinedType") and sl.PredefinedType else None,
            "is_external":     is_external,
            "u_value":         round(get_type_pset_value(sl, "Pset_SlabCommon", "ThermalTransmittance"), 4)
                               if get_type_pset_value(sl, "Pset_SlabCommon", "ThermalTransmittance") else None,
            "material":        material_str,
            "material_layers": material_layers,
            "area":            area,
            "thickness":       thickness,
            "floor_name":      get_floor_name(sl),
        })

    # ── Doors ───────────────────────────────────────────────────
    for d in model.by_type("IfcDoor"):
        w_m = d.OverallWidth  or 0
        h_m = d.OverallHeight or 0
        # FIX 5: width/height already in metres — no /1e6
        area = round(w_m * h_m, 4) if w_m and h_m else 0
        doors.append({
            "guid":        d.GlobalId,
            "name":        d.Name,
            "width":       w_m,
            "height":      h_m,
            "area":        area,
            "u_value":     get_type_pset_value(d, "Pset_DoorCommon", "ThermalTransmittance"),
            "fire_rating": get_type_pset_value(d, "Pset_DoorCommon", "FireRating"),
            "is_external": get_type_pset_value(d, "Pset_DoorCommon", "IsExternal"),
            "floor_name":  get_floor_name(d),
        })

    # ── Windows ─────────────────────────────────────────────────
    for win in model.by_type("IfcWindow"):
        w_m = win.OverallWidth  or 0
        h_m = win.OverallHeight or 0
        # FIX 5: width/height already in metres — no /1e6
        area = round(w_m * h_m, 4) if w_m and h_m else 0
        windows.append({
            "guid":        win.GlobalId,
            "name":        win.Name,
            "width":       w_m,
            "height":      h_m,
            "area":        area,
            "u_value":     get_type_pset_value(win, "Pset_WindowCommon", "ThermalTransmittance"),
            "is_external": get_type_pset_value(win, "Pset_WindowCommon", "IsExternal"),
            "floor_name":  get_floor_name(win),
        })

    # ── Furniture ───────────────────────────────────────────────
    for item in model.by_type("IfcFurnishingElement"):
        space_name = None
        for rel in model.by_type("IfcRelContainedInSpatialStructure"):
            if item in rel.RelatedElements and rel.RelatingStructure.is_a("IfcSpace"):
                space_name = rel.RelatingStructure.LongName or rel.RelatingStructure.Name
        furniture.append({"guid": item.GlobalId, "name": item.Name, "space_name": space_name})

    # ── Space Boundaries ────────────────────────────────────────
    # FIX 1+2+3: new dedup key includes start_x — keeps ALL segments per wall-room pair
    seen_bounds = set()
    seg_index   = {}  # wall_guid → counter

    for b in model.by_type("IfcRelSpaceBoundary"):
        elem  = b.RelatedBuildingElement
        space = b.RelatingSpace
        if not elem or not space:
            continue
        if b.PhysicalOrVirtualBoundary == "VIRTUAL":
            continue
        if not b.ConnectionGeometry:
            continue

        area, length, height, sx, sy, ex, ey = get_boundary_geometry(b)

        # FIX 1: include start_x in key so multiple segments kept
        key = (elem.GlobalId, space.Name, round(sx, 2) if sx is not None else None)
        if key in seen_bounds:
            continue
        seen_bounds.add(key)

        if elem.is_a("IfcWall") or elem.is_a("IfcWallStandardCase"):
            idx = seg_index.get(elem.GlobalId, 0)
            seg_index[elem.GlobalId] = idx + 1
            segments.append({
                "wall_guid":     elem.GlobalId,
                "wall_name":     elem.Name,
                "wall_type":     detect_wall_type(elem.Name),
                "space_name":    space.Name,
                "space_long":    space.LongName or space.Name,
                "area":          area,
                "length":        length,
                "height":        height,
                "start_x":       sx,
                "start_y":       sy,
                "end_x":         ex,
                "end_y":         ey,
                "segment_index": idx,
            })

        elif elem.is_a("IfcDoor"):
            w_m, h_m = elem.OverallWidth or 0, elem.OverallHeight or 0
            door_bounds.append({
                "door_guid":  elem.GlobalId,
                "door_name":  elem.Name,
                "space_name": space.Name,
                "space_long": space.LongName or space.Name,
                "width":      w_m,
                "height":     h_m,
                "area":       area or round(w_m * h_m, 4),
            })

        elif elem.is_a("IfcWindow"):
            w_m, h_m = elem.OverallWidth or 0, elem.OverallHeight or 0
            window_bounds.append({
                "window_guid": elem.GlobalId,
                "window_name": elem.Name,
                "space_name":  space.Name,
                "space_long":  space.LongName or space.Name,
                "width":       w_m,
                "height":      h_m,
                "area":        area or round(w_m * h_m, 4),
            })

        elif elem.is_a("IfcSlab"):
            slab_bounds.append({
                "slab_guid":  elem.GlobalId,
                "slab_name":  elem.Name,
                "space_name": space.Name,
                "space_long": space.LongName or space.Name,
                "area":       area,
            })

    # ── FIX 4: Hosted Elements (Wall → Window/Door) ──────────────
    seen_hosted = set()
    for wall in all_walls:
        if not hasattr(wall, "HasOpenings") or not wall.HasOpenings:
            continue
        for void_rel in wall.HasOpenings:
            opening = void_rel.RelatedOpeningElement
            if not opening or not hasattr(opening, "HasFillings"):
                continue
            for fill_rel in opening.HasFillings:
                filler = fill_rel.RelatedBuildingElement
                if not filler:
                    continue
                if not (filler.is_a("IfcWindow") or filler.is_a("IfcDoor")):
                    continue
                key = (wall.GlobalId, filler.GlobalId)
                if key in seen_hosted:
                    continue
                seen_hosted.add(key)
                hosted.append({
                    "wall_guid":    wall.GlobalId,
                    "wall_name":    wall.Name,
                    "element_guid": filler.GlobalId,
                    "element_name": filler.Name,
                    "element_type": filler.is_a(),
                    "opening_guid": opening.GlobalId,
                })

    print(f"✅ {len(floors)} floors | {len(spaces)} spaces | {len(walls)} walls | "
          f"{len(slabs)} slabs | {len(doors)} doors | {len(windows)} windows | "
          f"{len(furniture)} furniture")
    print(f"✅ {len(segments)} wall segments | {len(door_bounds)} door bounds | "
          f"{len(window_bounds)} window bounds | {len(slab_bounds)} slab bounds")
    print(f"✅ {len(hosted)} wall-hosted relationships (wall→window/door)")

    return {
        "floors":            floors,
        "spaces":            spaces,
        "walls":             walls,
        "slabs":             slabs,
        "doors":             doors,
        "windows":           windows,
        "furniture":         furniture,
        "segments":          segments,    # ← replaces "boundaries"
        "door_boundaries":   door_bounds,
        "window_boundaries": window_bounds,
        "slab_boundaries":   slab_bounds,
        "hosted_elements":   hosted,      # ← NEW
    }


# ── NEO4J UPLOAD ───────────────────────────────────────────────────────────

def upload(tx, query, items):
    for item in items:
        tx.run(query, **item)


def main():
    data = parse_ifc(IFC_PATH)

    with open("ifc_parsed.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print("\n📄 Saved ifc_parsed.json")

    with driver.session() as s:
        print("\n⬆️  Uploading nodes...")

        s.execute_write(upload, """
            MERGE (n:Floor {guid: $guid})
            SET n.name = $name, n.level = $level
        """, data["floors"])
        print(f"   ✅ {len(data['floors'])} Floors")

        s.execute_write(upload, """
            MERGE (n:Space {guid: $guid})
            SET n.name = $name, n.long_name = $long_name,
                n.area = $area, n.height = $height,
                n.usage = $usage
        """, data["spaces"])
        print(f"   ✅ {len(data['spaces'])} Spaces")

        s.execute_write(upload, """
            MERGE (n:Wall {guid: $guid})
            SET n.name = $name, n.is_external = $is_external,
                n.load_bearing = $load_bearing, n.u_value = $u_value,
                n.fire_rating = $fire_rating, n.material = $material,
                n.material_layers = $material_layers, n.wall_type = $wall_type,
                n.length = $length, n.height = $height, n.width = $width,
                n.area = $area
        """, data["walls"])
        print(f"   ✅ {len(data['walls'])} Walls")

        s.execute_write(upload, """
            MERGE (n:Slab {guid: $guid})
            SET n.name = $name, n.slab_type = $slab_type,
                n.is_external = $is_external, n.u_value = $u_value,
                n.material = $material, n.material_layers = $material_layers,
                n.area = $area, n.thickness = $thickness
        """, data["slabs"])
        print(f"   ✅ {len(data['slabs'])} Slabs")

        s.execute_write(upload, """
            MERGE (n:Door {guid: $guid})
            SET n.name = $name, n.width = $width, n.height = $height,
                n.area = $area, n.u_value = $u_value,
                n.fire_rating = $fire_rating, n.is_external = $is_external
        """, data["doors"])
        print(f"   ✅ {len(data['doors'])} Doors")

        s.execute_write(upload, """
            MERGE (n:Window {guid: $guid})
            SET n.name = $name, n.width = $width, n.height = $height,
                n.area = $area, n.u_value = $u_value, n.is_external = $is_external
        """, data["windows"])
        print(f"   ✅ {len(data['windows'])} Windows")

        s.execute_write(upload, """
            MERGE (n:Furniture {guid: $guid})
            SET n.name = $name, n.space_name = $space_name
        """, data["furniture"])
        print(f"   ✅ {len(data['furniture'])} Furniture")

        print("\n🔗 Creating floor relationships...")

        for rel_type, node_type, key in [
            ("CONTAINS_SPACE",  "Space",   "spaces"),
            ("CONTAINS_WALL",   "Wall",    "walls"),
            ("CONTAINS_SLAB",   "Slab",    "slabs"),
            ("CONTAINS_DOOR",   "Door",    "doors"),
            ("CONTAINS_WINDOW", "Window",  "windows"),
        ]:
            items = [x for x in data[key] if x.get("floor_name")]
            s.execute_write(upload, f"""
                MATCH (f:Floor  {{name: $floor_name}})
                MATCH (n:{node_type} {{guid: $guid}})
                MERGE (f)-[:{rel_type}]->(n)
            """, items)
            print(f"   ✅ Floor → {node_type}")

        print("\n🔗 Creating space boundary relationships...")

        # FIX 3: Create Segment nodes for Raumbuch (Purpose 2)
        for i, seg in enumerate(data["segments"]):
            s.execute_write(lambda tx, seg=seg, i=i: tx.run("""
                MATCH (w:Wall  {guid: $wall_guid})
                MATCH (sp:Space {name: $space_name})
                CREATE (seg:Segment {
                    guid:          $seg_guid,
                    wall_guid:     $wall_guid,
                    wall_type:     $wall_type,
                    area:          $area,
                    length:        $length,
                    height:        $height,
                    start_x:       $start_x,
                    start_y:       $start_y,
                    end_x:         $end_x,
                    end_y:         $end_y,
                    segment_index: $segment_index
                })
                MERGE (w)-[:HAS_SEGMENT]->(seg)
                MERGE (seg)-[:FACES]->(sp)
                MERGE (w)-[:BOUNDS]->(sp)
            """,
                seg_guid      = f"{seg['wall_guid']}_{i}",
                wall_guid     = seg["wall_guid"],
                wall_type     = seg["wall_type"],
                space_name    = seg["space_name"],
                area          = seg["area"],
                length        = seg["length"],
                height        = seg["height"],
                start_x       = seg["start_x"],
                start_y       = seg["start_y"],
                end_x         = seg["end_x"],
                end_y         = seg["end_y"],
                segment_index = seg["segment_index"],
            ))
        print(f"   ✅ {len(data['segments'])} Wall Segments + BOUNDS relationships")

        s.execute_write(upload, """
            MATCH (d:Door  {guid: $door_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (d)-[r:OPENS_INTO]->(s)
            SET r.area = $area, r.width = $width, r.height = $height
        """, data["door_boundaries"])
        print(f"   ✅ {len(data['door_boundaries'])} Door → Space")

        s.execute_write(upload, """
            MATCH (w:Window {guid: $window_guid})
            MATCH (s:Space  {name: $space_name})
            MERGE (w)-[r:FACES]->(s)
            SET r.area = $area, r.width = $width, r.height = $height
        """, data["window_boundaries"])
        print(f"   ✅ {len(data['window_boundaries'])} Window → Space")

        s.execute_write(upload, """
            MATCH (sl:Slab {guid: $slab_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (sl)-[r:COVERS]->(s)
            SET r.area = $area
        """, data["slab_boundaries"])
        print(f"   ✅ {len(data['slab_boundaries'])} Slab → Space")

        for f in data["furniture"]:
            if f["space_name"]:
                s.execute_write(lambda tx, f=f: tx.run("""
                    MATCH (i:Furniture {guid: $guid})
                    MATCH (sp:Space {long_name: $space_name})
                    MERGE (i)-[:LOCATED_IN]->(sp)
                """, **f))
        print(f"   ✅ Furniture → Space")

        # FIX 4: Wall HOSTS Window/Door
        print("\n🔗 Creating wall hosting relationships...")
        s.execute_write(upload, """
            MATCH (w:Wall {guid: $wall_guid})
            MATCH (e)
            WHERE (e:Window OR e:Door) AND e.guid = $element_guid
            MERGE (w)-[r:HOSTS]->(e)
            SET r.element_type = $element_type,
                r.opening_guid = $opening_guid
        """, data["hosted_elements"])
        print(f"   ✅ {len(data['hosted_elements'])} Wall → Window/Door (HOSTS)")

    driver.close()
    print("\n🎉 Neo4j database ready!")
    print("\n── Example queries ──────────────────────────────────────")
    print("// Raumbuch: area per wall type per room")
    print("MATCH (w:Wall)-[:HAS_SEGMENT]->(seg:Segment)-[:FACES]->(s:Space)")
    print("RETURN s.long_name, w.wall_type, SUM(seg.area) AS area\n")
    print("// QTO: total drywall area")
    print("MATCH (w:Wall {wall_type:'drywall'}) RETURN SUM(w.area)\n")
    print("// Which wall hosts which window")
    print("MATCH (w:Wall)-[:HOSTS]->(win:Window) RETURN w.name, win.name")


if __name__ == "__main__":
    main()
