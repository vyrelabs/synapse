// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package parser

import (
	"errors"
	"io"

	"github.com/PuerkitoBio/goquery"
	"github.com/vyrelabs/synapse/extract/transform"
)

type Element struct {
	dom *goquery.Selection
}

func NewElement(r io.Reader) (*Element, error) {
	doc, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return nil, err
	}

	if doc.Selection == nil {
		return nil, errors.New("extract: nil selection")
	}

	return &Element{
		dom: doc.Selection,
	}, nil
}

func (e *Element) TagName() string {
	return goquery.NodeName(e.dom)
}

func (e *Element) Attr(attr string) (string, bool) {
	return e.dom.Attr(attr)
}

func (e *Element) HasClass(class string) bool {
	return e.dom.HasClass(class)
}

func (e *Element) Text() string {
	return transform.ApplyTransformations(e.dom.Text(), transform.TrimSpace, transform.NormalizeWhitespace)
}

func (e *Element) RawText() string {
	return e.dom.Text()
}

func (e *Element) Find(selector string) *Element {
	return newElementFromSelection(e.dom.Find(selector))
}

func (e *Element) Collect(selector string, transformers ...transform.Transformer) string {
	element := e.Find(selector)
	text := element.RawText()
	return transform.ApplyTransformations(text, transformers...)
}

func (e *Element) CollectAll(selector string, transformers ...transform.Transformer) []string {
	return e.Iter(selector).Map(func(e *Element) string {
		return transform.ApplyTransformations(e.Text(), transformers...)
	})
}

func (e *Element) Iter(selector string) *elementIter {
	return &elementIter{
		dom: e.dom.Find(selector),
	}
}

type elementIter struct {
	dom *goquery.Selection
}

func (e *elementIter) Each(fn func(*Element)) {
	if fn == nil {
		return
	}
	if e.dom == nil {
		return
	}
	e.dom.Each(func(_ int, selection *goquery.Selection) {
		fn(newElementFromSelection(selection))
	})
}

func (e *elementIter) Map(fn func(*Element) string) []string {
	if fn == nil {
		return []string{}
	}
	if e.dom == nil {
		return []string{}
	}
	results := make([]string, 0, e.dom.Length())
	e.Each(func(el *Element) {
		results = append(results, fn(el))
	})
	return results
}

// SAFETY: The caller must ensure that the selection is valid.
func newElementFromSelection(selection *goquery.Selection) *Element {
	return &Element{
		dom: selection,
	}
}
