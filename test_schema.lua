
box.schema.space.create('test1', {id = 512});
box.space.test1:create_index('primary', {
	type = 'HASH',
	parts = {1, 'unsigned'}
});

box.schema.space.create('test2', {
	id = 513,
	field_count = 49
});
box.space.test2:create_index('primary', {
	type = 'HASH',
	parts = {1, 'unsigned'}
});
box.space.test2:create_index('email', {
	unique = false,
	parts = {2, 'string'}
});
box.space.test2:create_index('login', {
	unique = false,
	parts = {3, 'string'}
});
