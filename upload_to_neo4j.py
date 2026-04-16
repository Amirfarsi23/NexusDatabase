import ifcopenshell
import ifcopenshell.util.element as util
import math
import json
from neo4j import GraphDatabase

# --- CONFIG ---
NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "Amir44813141-"
IFC_PATH       = "C:\\Nextcloud\\Promotion\\NexusDatabase\\sample_data\\bimnexus.ifc"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


# ── HELPERS ────────────────────────────────────────────────────

def detect_wall_type(wall_name):
    name = (wall_name or "").lower()
    if "ceramic" in name or "fliese" in name or "tile" in name:  return "ceramic"
    elif "glas"  in name or "glass"  in name:                    return "glass"
    elif "paint" in name or "farbe"  in name:                    return "paint"
    elif "gk"    in name or "gips"   in name or "trockenbau" in name: return "drywall"
    elif "ziegel" in name or "beton" in name or "concrete" in name:   return "structural"
    else:                                                         return "general"


def get_type_pset_value(element, pset_name, prop_name):
    """Get a property from the element's TYPE-level property sets."""
    for definition in element.IsDefinedBy:
        if definition.is_a("IfcRelDefinesByType"):
            element_type = definition.RelatingType
            if not hasattr(element_type, "HasPropertySets"):
                continue
            for pset in element_type.HasPropertySets:
                if pset.Name == pset_name:
                    for prop in pset.HasProperties:
                        if prop.Name == prop_name:
                            try:
                                return prop.NominalValue.wrappedValue
                            except:
                                return None
    return None


def get_instance_pset_value(element, pset_name, prop_name):
    """Get a property from the element's INSTANCE-level property sets."""
    psets = util.get_psets(element)
    for name, props in psets.items():
        if name == pset_name and prop_name in props:
            return props[prop_name]
    return None


def get_material_info(element):
    """Return (material_str, material_layers_json)."""
    mat = util.get_material(element)
    material_str    = None
    material_layers = []

    if not mat:
        return None, "[]"

    if hasattr(mat, "Name") and mat.Name:
        material_str = mat.Name

    elif hasattr(mat, "ForLayerSet"):
        layers = mat.ForLayerSet.MaterialLayers
        parts  = []
        for layer in layers:
            if layer.Material:
                thickness_mm = round(layer.LayerThickness * 1000, 1)
                name         = layer.Material.Name
                parts.append(f"{name}:{thickness_mm}mm")
                material_layers.append({"name": name, "thickness_mm": thickness_mm})
        material_str = " | ".join(parts)

    elif hasattr(mat, "MaterialConstituents"):
        parts = []
        for constituent in mat.MaterialConstituents:
            if constituent.Material:
                name = constituent.Material.Name
                parts.append(name)
                material_layers.append({"name": name, "thickness_mm": None})
        material_str = " | ".join(parts)

    return material_str, json.dumps(material_layers)


def polygon_area(points):
    """Shoelace formula for 2D polygon area."""
    n    = len(points)
    area = 0.0
    for i in range(n):
        j     = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    return abs(area) / 2.0


def get_boundary_area(boundary):
    """Calculate boundary face area from IFC geometry — handles both surface types."""
    try:
        geom = boundary.ConnectionGeometry.SurfaceOnRelatingElement
        if geom.is_a("IfcCurveBoundedPlane"):
            pts = [(p.Coordinates[0], p.Coordinates[1])
                   for p in geom.OuterBoundary.Points]
            return round(polygon_area(pts), 4)
        elif geom.is_a("IfcSurfaceOfLinearExtrusion"):
            depth   = geom.Depth
            profile = geom.SweptCurve
            if hasattr(profile, "Curve") and hasattr(profile.Curve, "Points"):
                pts    = [(p.Coordinates[0], p.Coordinates[1])
                          for p in profile.Curve.Points]
                length = sum(
                    math.sqrt((pts[i+1][0]-pts[i][0])**2 +
                              (pts[i+1][1]-pts[i][1])**2)
                    for i in range(len(pts)-1)
                )
                return round(length * depth, 4)
    except:
        pass
    return None


# ── IFC PARSER ─────────────────────────────────────────────────

def parse_ifc(file_path):
    print(f"📂 Opening IFC: {file_path}")
    model = ifcopenshell.open(file_path)

    floors    = []
    spaces    = []
    walls     = []
    slabs     = []
    doors     = []
    windows   = []
    furniture = []
    boundaries        = []
    door_bounds       = []
    window_bounds     = []
    slab_bounds       = []

    # Floors
    for f in model.by_type("IfcBuildingStorey"):
        floors.append({
            "guid":  f.GlobalId,
            "name":  f.Name,
            "level": f.Elevation or 0
        })

    # Spaces
    for s in model.by_type("IfcSpace"):
        psets  = util.get_psets(s)
        area   = None
        height = None
        for p in psets.values():
            if "NetFloorArea"        in p: area   = round(p["NetFloorArea"], 2)
            if "FinishCeilingHeight" in p: height = p["FinishCeilingHeight"]
            if "Height"              in p: height = p["Height"]
        usage = get_instance_pset_value(s, "Pset_SpaceCommon", "OccupancyType")
        spaces.append({
            "guid":      s.GlobalId,
            "name":      s.Name,
            "long_name": s.LongName or s.Name,
            "area":      area,
            "height":    height,
            "usage":     usage
        })

    # Walls
    seen = set()
    for w in model.by_type("IfcWall") + model.by_type("IfcWallStandardCase"):
        if w.GlobalId in seen:
            continue
        seen.add(w.GlobalId)

        psets        = util.get_psets(w)
        is_external  = False
        load_bearing = False
        for p in psets.values():
            if "IsExternal"  in p: is_external  = p["IsExternal"]
            if "LoadBearing" in p: load_bearing = p["LoadBearing"]

        u_value     = get_type_pset_value(w, "Pset_WallCommon", "ThermalTransmittance")
        fire_rating = get_type_pset_value(w, "Pset_WallCommon", "FireRating")
        if not fire_rating:
            fire_rating = get_instance_pset_value(w, "Pset_WallCommon", "FireRating")

        material_str, material_layers = get_material_info(w)

        length = height = width = None
        for p in psets.values():
            if "Length" in p: length = round(p["Length"], 3)
            if "Height" in p: height = round(p["Height"], 3)
            if "Width"  in p: width  = round(p["Width"],  3)

        walls.append({
            "guid":            w.GlobalId,
            "name":            w.Name,
            "is_external":     is_external,
            "load_bearing":    load_bearing,
            "u_value":         round(u_value, 4) if u_value else None,
            "fire_rating":     fire_rating,
            "material":        material_str,
            "material_layers": material_layers,
            "wall_type":       detect_wall_type(w.Name),
            "length":          length,
            "height":          height,
            "width":           width
        })

    # Slabs
    seen_slabs = set()
    for sl in model.by_type("IfcSlab"):
        if sl.GlobalId in seen_slabs:
            continue
        seen_slabs.add(sl.GlobalId)

        psets       = util.get_psets(sl)
        is_external = False
        for p in psets.values():
            if "IsExternal" in p: is_external = p["IsExternal"]

        u_value  = get_type_pset_value(sl, "Pset_SlabCommon", "ThermalTransmittance")
        material_str, material_layers = get_material_info(sl)

        area      = None
        thickness = None
        for p in psets.values():
            if "NetArea"   in p: area      = round(p["NetArea"],   2)
            if "GrossArea" in p and not area: area = round(p["GrossArea"], 2)
            if "Depth"     in p: thickness = round(p["Depth"],     3)
            if "Width"     in p and not thickness: thickness = round(p["Width"], 3)

        slab_type = str(sl.PredefinedType) if hasattr(sl, "PredefinedType") and sl.PredefinedType else None

        slabs.append({
            "guid":            sl.GlobalId,
            "name":            sl.Name,
            "slab_type":       slab_type,
            "is_external":     is_external,
            "u_value":         round(u_value, 4) if u_value else None,
            "material":        material_str,
            "material_layers": material_layers,
            "area":            area,
            "thickness":       thickness
        })

    # Doors
    for d in model.by_type("IfcDoor"):
        w, h    = d.OverallWidth or 0, d.OverallHeight or 0
        u_value = get_type_pset_value(d, "Pset_DoorCommon", "ThermalTransmittance")
        doors.append({
            "guid":        d.GlobalId,
            "name":        d.Name,
            "width":       w,
            "height":      h,
            "area":        round((w * h) / 1e6, 4) if w and h else 0,
            "u_value":     round(u_value, 4) if u_value else None,
            "fire_rating": get_type_pset_value(d, "Pset_DoorCommon", "FireRating"),
            "is_external": get_type_pset_value(d, "Pset_DoorCommon", "IsExternal")
        })

    # Windows
    for win in model.by_type("IfcWindow"):
        w, h    = win.OverallWidth or 0, win.OverallHeight or 0
        u_value = get_type_pset_value(win, "Pset_WindowCommon", "ThermalTransmittance")
        windows.append({
            "guid":        win.GlobalId,
            "name":        win.Name,
            "width":       w,
            "height":      h,
            "area":        round((w * h) / 1e6, 4) if w and h else 0,
            "u_value":     round(u_value, 4) if u_value else None,
            "is_external": get_type_pset_value(win, "Pset_WindowCommon", "IsExternal")
        })

    # Furniture
    for item in model.by_type("IfcFurnishingElement"):
        space_name = None
        for rel in model.by_type("IfcRelContainedInSpatialStructure"):
            if item in rel.RelatedElements and rel.RelatingStructure.is_a("IfcSpace"):
                space_name = rel.RelatingStructure.LongName or rel.RelatingStructure.Name
        furniture.append({
            "guid":       item.GlobalId,
            "name":       item.Name,
            "space_name": space_name
        })

    # Space boundaries
    seen2 = set()
    for b in model.by_type("IfcRelSpaceBoundary"):
        elem  = b.RelatedBuildingElement
        space = b.RelatingSpace
        if not elem or not space:
            continue
        if b.PhysicalOrVirtualBoundary == "VIRTUAL":
            continue
        key = (elem.GlobalId, space.Name)
        if key in seen2:
            continue
        seen2.add(key)

        area = get_boundary_area(b)

        if elem.is_a("IfcWall") or elem.is_a("IfcWallStandardCase"):
            boundaries.append({
                "wall_guid":  elem.GlobalId,
                "wall_name":  elem.Name,
                "wall_type":  detect_wall_type(elem.Name),
                "space_name": space.Name,
                "space_long": space.LongName or space.Name,
                "area":       area
            })
        elif elem.is_a("IfcDoor"):
            w, h = elem.OverallWidth or 0, elem.OverallHeight or 0
            door_bounds.append({
                "door_guid":  elem.GlobalId,
                "door_name":  elem.Name,
                "space_name": space.Name,
                "space_long": space.LongName or space.Name,
                "width":      w,
                "height":     h,
                "area":       area or round((w * h) / 1e6, 4)
            })
        elif elem.is_a("IfcWindow"):
            w, h = elem.OverallWidth or 0, elem.OverallHeight or 0
            window_bounds.append({
                "window_guid": elem.GlobalId,
                "window_name": elem.Name,
                "space_name":  space.Name,
                "space_long":  space.LongName or space.Name,
                "width":       w,
                "height":      h,
                "area":        area or round((w * h) / 1e6, 4)
            })
        elif elem.is_a("IfcSlab"):
            slab_bounds.append({
                "slab_guid":  elem.GlobalId,
                "slab_name":  elem.Name,
                "space_name": space.Name,
                "space_long": space.LongName or space.Name,
                "area":       area
            })

    print(f"✅ {len(floors)} floors | {len(spaces)} spaces | {len(walls)} walls | "
          f"{len(slabs)} slabs | {len(doors)} doors | {len(windows)} windows | "
          f"{len(furniture)} furniture")
    print(f"✅ {len(boundaries)} wall bounds | {len(door_bounds)} door bounds | "
          f"{len(window_bounds)} window bounds | {len(slab_bounds)} slab bounds")

    return {
        "floors":            floors,
        "spaces":            spaces,
        "walls":             walls,
        "slabs":             slabs,
        "doors":             doors,
        "windows":           windows,
        "furniture":         furniture,
        "boundaries":        boundaries,
        "door_boundaries":   door_bounds,
        "window_boundaries": window_bounds,
        "slab_boundaries":   slab_bounds
    }


# ── NEO4J UPLOAD ───────────────────────────────────────────────

def upload(tx, query, items):
    for item in items:
        tx.run(query, **item)


def main():
    data = parse_ifc(IFC_PATH)

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
                n.area = $area, n.height = $height, n.usage = $usage
        """, data["spaces"])
        print(f"   ✅ {len(data['spaces'])} Spaces")

        s.execute_write(upload, """
            MERGE (n:Wall {guid: $guid})
            SET n.name = $name, n.is_external = $is_external,
                n.load_bearing = $load_bearing, n.u_value = $u_value,
                n.fire_rating = $fire_rating, n.material = $material,
                n.material_layers = $material_layers, n.wall_type = $wall_type,
                n.length = $length, n.height = $height, n.width = $width
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

        print("\n🔗 Creating relationships...")

        s.execute_write(upload, """
            MATCH (w:Wall  {guid: $wall_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (w)-[r:BOUNDS]->(s)
            SET r.area = $area, r.wall_type = $wall_type
        """, data["boundaries"])
        print(f"   ✅ {len(data['boundaries'])} Wall-Space")

        s.execute_write(upload, """
            MATCH (d:Door  {guid: $door_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (d)-[r:OPENS_INTO]->(s)
            SET r.area = $area, r.width = $width, r.height = $height
        """, data["door_boundaries"])
        print(f"   ✅ {len(data['door_boundaries'])} Door-Space")

        s.execute_write(upload, """
            MATCH (w:Window {guid: $window_guid})
            MATCH (s:Space  {name: $space_name})
            MERGE (w)-[r:FACES]->(s)
            SET r.area = $area, r.width = $width, r.height = $height
        """, data["window_boundaries"])
        print(f"   ✅ {len(data['window_boundaries'])} Window-Space")

        s.execute_write(upload, """
            MATCH (sl:Slab {guid: $slab_guid})
            MATCH (s:Space {name: $space_name})
            MERGE (sl)-[r:COVERS]->(s)
            SET r.area = $area
        """, data["slab_boundaries"])
        print(f"   ✅ {len(data['slab_boundaries'])} Slab-Space")

        for f in data["furniture"]:
            if f["space_name"]:
                s.execute_write(lambda tx, f=f: tx.run("""
                    MATCH (i:Furniture {guid: $guid})
                    MATCH (sp:Space    {long_name: $space_name})
                    MERGE (i)-[:LOCATED_IN]->(sp)
                """, **f))
        print(f"   ✅ Furniture-Space links")

    driver.close()
    print("\n🎉 NexusDatabase is ready!")


if __name__ == "__main__":
    main()
