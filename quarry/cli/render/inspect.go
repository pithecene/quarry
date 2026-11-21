package render

import (
	"fmt"
	"reflect"
	"text/tabwriter"

	"github.com/pithecene/quarry/cli/reader"
)

func (r *Renderer) renderInspectRunTable(resp *reader.InspectRunResponse) error {
	w := tabwriter.NewWriter(r.out, 0, 0, 2, ' ', 0)
	if err := r.writeStructRows(w, reflect.ValueOf(resp.Run)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "policy:\t%s\n", r.formatValue(reflect.ValueOf(resp.Policy))); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if len(resp.SidecarFiles) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(r.out, "\nsidecar_files:"); err != nil {
		return err
	}
	return r.renderSliceTable(reflect.ValueOf(resp.SidecarFiles))
}
