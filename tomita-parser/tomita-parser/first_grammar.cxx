#encoding "utf8"

born -> Verb<kwtype=born>;
city -> Noun<kwtype=city>;
Person -> AnyWord<gram="имя">;
S -> Person interp(BornFact.Person) born "f" city interp(BornFact.Place);
