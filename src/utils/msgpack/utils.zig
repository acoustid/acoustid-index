pub fn NonOptional(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info == .Optional) {
        return type_info.Optional.child;
    }
    return T;
}
