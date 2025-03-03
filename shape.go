package no6

// TODO: Schemas or something along those lines? Maybe don't call it schemas
// because it won't be like a normal schema. Shapes?
//
// Anyway, I want to get rid of the ability to do (X, ?, Y) queries, as you
// _should_ know the predicates you are looking for.
//
// Is a good plan to do struct tags?
//
//   type Person struct {
//     Name string `no6:"name"`
//     Age int `no6:"age"`
//   }
//
// Then db.Query("john", &person, Eq, Anything) or something would fill in the gaps?
//
// Or have it as a plain list for now?
//
//   db.Query("john", []string{"name", "age"}, Eq, Anything) //=> current output
//
// But that would mean all the type annoyingness.
