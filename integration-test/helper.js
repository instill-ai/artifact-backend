export function deepEqual(x, y) {
  const ok = Object.keys,
    tx = typeof x,
    ty = typeof y;
  return x && y && tx === "object" && tx === ty
    ? ok(x).length === ok(y).length &&
    ok(x).every((key) => deepEqual(x[key], y[key]))
    : x === y;
}

export function isUUID(uuid) {
  const regexExp =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return regexExp.test(uuid);
}

export function isValidOwner(owner, expectedOwner) {
  if (owner === null || owner === undefined) return false;
  if (owner.user === null || owner.user === undefined) return false;
  if (owner.user.id !== "admin") return false;
  return deepEqual(owner.user.profile, expectedOwner.profile)
}

export function validateCatalog(catalog, isPrivate) {
  if (!("catalogUid" in catalog)) {
    console.log("Catalog has no catalog_uid field");
    return false;
  }

  if (!("catalogId" in catalog)) {
    console.log("Catalog has no catalog_id field");
    return false;
  }

  if (!("name" in catalog)) {
    console.log("Catalog has no name field");
    return false;
  }

  if (!("description" in catalog)) {
    console.log("Catalog has no description field");
    return false;
  }

  if (!("createTime" in catalog)) {
    console.log("Catalog has no create_time field");
    return false;
  }

  if (!("updateTime" in catalog)) {
    console.log("Catalog has no update_time field");
    return false;
  }

  return true;
}

export function validateFile(file, isPrivate) {
  if (!("fileUid" in file)) {
    console.log("File has no file_uid field");
    return false;
  }

  if (!("name" in file)) {
    console.log("File has no name field");
    return false;
  }

  if (!("type" in file)) {
    console.log("File has no type field");
    return false;
  }

  if (!("processStatus" in file)) {
    console.log("File has no process_status field");
    return false;
  }

  if (!("createTime" in file)) {
    console.log("File has no create_time field");
    return false;
  }

  if (!("updateTime" in file)) {
    console.log("File has no update_time field");
    return false;
  }

  return true;
}

export function validateCatalogGRPC(catalog, isPrivate) {
  if (!("catalogUid" in catalog)) {
    console.log("Catalog has no catalogUid field");
    return false;
  }

  if (!("catalogId" in catalog)) {
    console.log("Catalog has no catalogId field");
    return false;
  }

  if (!("name" in catalog)) {
    console.log("Catalog has no name field");
    return false;
  }

  if (!("description" in catalog)) {
    console.log("Catalog has no description field");
    return false;
  }

  if (!("createTime" in catalog)) {
    console.log("Catalog has no createTime field");
    return false;
  }

  if (!("updateTime" in catalog)) {
    console.log("Catalog has no updateTime field");
    return false;
  }

  return true;
}

export function validateFileGRPC(file, isPrivate) {
  if (!("fileUid" in file)) {
    console.log("File has no fileUid field");
    return false;
  }

  if (!("name" in file)) {
    console.log("File has no name field");
    return false;
  }

  if (!("type" in file)) {
    console.log("File has no type field");
    return false;
  }

  if (!("processStatus" in file)) {
    console.log("File has no processStatus field");
    return false;
  }

  if (!("createTime" in file)) {
    console.log("File has no createTime field");
    return false;
  }

  if (!("updateTime" in file)) {
    console.log("File has no updateTime field");
    return false;
  }

  return true;
}
