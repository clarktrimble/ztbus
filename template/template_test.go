package template

// Todo: fix!!
/*
func TestTemplate_Load(t *testing.T) {
	type fields struct {
		Dir    string
		Suffix string
		Left   string
		Right  string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"check that Load, um, loads",
			fields{"../../container/nomad", "tmpl", "<%", "%>"},
			false,
		},
		{
			"check that Load relays error from below",
			fields{"../../container/nomad", "brgl", "<%", "%>"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := &Template{
				Path:   tt.fields.Dir,
				Suffix: tt.fields.Suffix,
				Left:   tt.fields.Left,
				Right:  tt.fields.Right,
			}
			err := tmpl.Load()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, tmpl.tmpl.Templates(), 2)
			}
		})
	}
}

func TestTemplate_RenderString(t *testing.T) {
	type fields struct {
		Dir    string
		Suffix string
		Left   string
		Right  string
	}
	type args struct {
		name string
		data interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			"check thqt RenderString renders a string",
			fields{"../../container/nomad", "tmpl", "<%", "%>"},
			args{"nomad-job-periodic.hcl", map[string]interface{}{
				"kind":          "t1dedup",
				"dc":            "sjc1",
				"cronday":       "1",
				"jobnameprefix": "t1",
				"action":        "deduplicate",
				"cronmin":       "15",
			}},
			`job "t1-deduplicate-sjc1"`,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := &Template{
				Path:   tt.fields.Dir,
				Suffix: tt.fields.Suffix,
				Left:   tt.fields.Left,
				Right:  tt.fields.Right,
			}
			err := tmpl.Load()
			assert.NoError(t, err)

			got, err := tmpl.RenderString(tt.args.name, tt.args.data)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, got, tt.want)
			}
		})
	}
}
*/
