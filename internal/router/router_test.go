package router

import "testing"

func TestParseTarget(t *testing.T) {
	cases := []struct {
		target string
		svc    string
		method string
	}{
		{"Hello.Greeter/SayHello", "Hello.Greeter", "SayHello"},
		{"order_service/CreateOrder", "order_service", "CreateOrder"},
		{"only_service", "only_service", ""},
		{"too/many/parts", "too/many/parts", ""},
	}

	for _, c := range cases {
		svc, method := parseTarget(c.target)
		if svc != c.svc || method != c.method {
			t.Errorf("parseTarget(%q) = (%q, %q), want (%q, %q)",
				c.target, svc, method, c.svc, c.method)
		}
	}
}
