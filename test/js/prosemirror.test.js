import ProseMirrorEditor, { XMLView } from '@/prosemirror-editor.ts';

describe('ProseMirrorEditor', () => {
  let container;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
  });

  afterEach(() => {
    document.body.removeChild(container);
  });

  test('initializes with simple XML content', () => {
    const xml = '<page><p>Hello world</p></page>';
    const editor = new ProseMirrorEditor(container, xml);

    expect(editor.view).toBeDefined();
    expect(editor.view.state.doc.childCount).toBe(1);

    const firstBlock = editor.view.state.doc.child(0);
    expect(firstBlock.attrs.type).toBe('p');
    expect(firstBlock.textContent).toBe('Hello world');

    editor.destroy();
  });

  test('getText returns XML content', () => {
    const xml = '<page><p>Test content</p></page>';
    const editor = new ProseMirrorEditor(container, xml);

    const output = editor.getText();
    expect(output).toContain('<page>');
    expect(output).toContain('<p>Test content</p>');

    editor.destroy();
  });

  test('toggleMark adds error mark to selection', () => {
    const xml = '<page><p>Hello world</p></page>';
    const editor = new ProseMirrorEditor(container, xml);

    // Select "world" - positions are: 'w' at 7, end of "world" at 12
    const { TextSelection } = require('prosemirror-state');
    const tr = editor.view.state.tr.setSelection(
      TextSelection.create(editor.view.state.doc, 7, 12)
    );
    editor.view.updateState(editor.view.state.apply(tr));

    editor.toggleMark('error');
    const output = editor.getText();
    expect(output).toContain('<error>world</error>');

    editor.destroy();
  });

  test('setText updates editor content', () => {
    const editor = new ProseMirrorEditor(container, '<page><p>Initial</p></page>');

    editor.setText('<page><p>Updated</p></page>');

    expect(editor.view.state.doc.child(0).textContent).toBe('Updated');

    editor.destroy();
  });

  test('Shift-Enter creates a new block below', () => {
    const xml = '<page><p>First block</p></page>';
    const editor = new ProseMirrorEditor(container, xml);

    const { from, to } = editor.view.state.selection;
    const shiftEnterEvent = new KeyboardEvent('keydown', {
      key: 'Enter',
      shiftKey: true,
      bubbles: true,
    });

    editor.view.dom.dispatchEvent(shiftEnterEvent);

    const output = editor.getText();
    expect(output).toContain('<p>First block</p>');
    expect(output).toContain('<p></p>');
    expect(editor.view.state.doc.childCount).toBe(2);

    editor.destroy();
  });

  test('Shift-Enter preserves content in current block', () => {
    const xml = '<page><p>Content here</p></page>';
    const editor = new ProseMirrorEditor(container, xml);

    const { TextSelection } = require('prosemirror-state');
    const tr = editor.view.state.tr.setSelection(
      TextSelection.create(editor.view.state.doc, 8, 8)
    );
    editor.view.updateState(editor.view.state.apply(tr));

    const shiftEnterEvent = new KeyboardEvent('keydown', {
      key: 'Enter',
      shiftKey: true,
      bubbles: true,
    });

    editor.view.dom.dispatchEvent(shiftEnterEvent);

    const output = editor.getText();
    expect(output).toContain('<p>Content here</p>');
    expect(editor.view.state.doc.childCount).toBe(2);

    editor.destroy();
  });
});

describe('XMLView', () => {
  let container;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
  });

  afterEach(() => {
    document.body.removeChild(container);
  });

  test('initializes with XML content', () => {
    const xml = '<page><p>Hello world</p></page>';
    const xmlView = new XMLView(container, xml);

    expect(xmlView.view).toBeDefined();
    expect(xmlView.view.state.doc).toBeDefined();
    expect(xmlView.getText()).toBe(xml);

    xmlView.destroy();
  });

  test('document has codeblock structure', () => {
    const xml = '<page><p>Test</p></page>';
    const xmlView = new XMLView(container, xml);

    const doc = xmlView.view.state.doc;
    expect(doc.childCount).toBe(1);
    expect(doc.child(0).type.name).toBe('codeblock');
    expect(doc.child(0).textContent).toBe(xml);

    xmlView.destroy();
  });

  test('getText returns XML content', () => {
    const xml = '<page><p>Hello world</p></page>';
    const xmlView = new XMLView(container, xml);

    expect(xmlView.getText()).toBe(xml);

    xmlView.destroy();
  });

  test('setText updates content', () => {
    const xml1 = '<page><p>First</p></page>';
    const xml2 = '<page><p>Second</p></page>';
    const xmlView = new XMLView(container, xml1);

    xmlView.setText(xml2);

    expect(xmlView.getText()).toBe(xml2);
    expect(xmlView.view.state.doc.textContent).toBe(xml2);

    xmlView.destroy();
  });

  test('decorations are created for XML tags', () => {
    const xml = '<page><p>Hello</p></page>';
    const xmlView = new XMLView(container, xml);

    const decorationPlugin = xmlView.view.state.plugins.find(
      p => p.spec && p.spec.props && p.spec.props.decorations
    );
    expect(decorationPlugin).toBeDefined();

    const decorations = decorationPlugin.getState(xmlView.view.state);
    expect(decorations).toBeDefined();

    const allDecorations = decorations.find();
    expect(allDecorations.length).toBe(4);

    xmlView.destroy();
  });

  test('decorations cover all XML tags', () => {
    const xml = '<page><p>Test</p></page>';
    const xmlView = new XMLView(container, xml);

    const decorationPlugin = xmlView.view.state.plugins.find(
      p => p.spec && p.spec.props && p.spec.props.decorations
    );

    const decorations = decorationPlugin.getState(xmlView.view.state);
    const allDecorations = decorations.find();

    // Should have decorations for: <page>, <p>, </p>, </page> = 4 tags
    expect(allDecorations.length).toBe(4);

    xmlView.destroy();
  });

  test('decorations have color style', () => {
    const xml = '<page><p>Test</p></page>';
    const xmlView = new XMLView(container, xml);

    const decorationPlugin = xmlView.view.state.plugins.find(
      p => p.spec && p.spec.props && p.spec.props.decorations
    );

    const decorations = decorationPlugin.getState(xmlView.view.state);
    const allDecorations = decorations.find();

    const hasColorStyle = allDecorations.some(deco =>
      deco.type.attrs && deco.type.attrs.style && deco.type.attrs.style.includes('color')
    );
    expect(hasColorStyle).toBe(true);

    xmlView.destroy();
  });

  test('decorations update when text changes', () => {
    const xml1 = '<page></page>';
    const xml2 = '<page><p>New</p><verse>Content</verse></page>';
    const xmlView = new XMLView(container, xml1);

    const decorationPlugin = xmlView.view.state.plugins.find(
      p => p.spec && p.spec.props && p.spec.props.decorations
    );

    let decorations = decorationPlugin.getState(xmlView.view.state);
    let allDecorations = decorations.find();
    const initialCount = allDecorations.length;

    xmlView.setText(xml2);

    decorations = decorationPlugin.getState(xmlView.view.state);
    allDecorations = decorations.find();

    // More tags in xml2, so should have more decorations
    expect(allDecorations.length).toBeGreaterThan(initialCount);

    xmlView.destroy();
  });

  test('focus sets focus on the editor', () => {
    const xml = '<page><p>Test</p></page>';
    const xmlView = new XMLView(container, xml);

    xmlView.focus();

    expect(xmlView.view.hasFocus()).toBe(true);

    xmlView.destroy();
  });

  test('handles empty content', () => {
    const xmlView = new XMLView(container, '');

    expect(xmlView.getText()).toBe('');
    expect(xmlView.view.state.doc.textContent).toBe('');

    xmlView.destroy();
  });


  test('preserves whitespace and newlines', () => {
    const xml = `<page>
<p>Line 1
Line 2</p>
</page>`;
    const xmlView = new XMLView(container, xml);

    expect(xmlView.getText()).toBe(xml);
    expect(xmlView.getText()).toContain('\n');

    xmlView.destroy();
  });
});
