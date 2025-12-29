import { EditorState, Plugin, Transaction, Selection } from 'prosemirror-state';
import { EditorView, Decoration, DecorationSet } from 'prosemirror-view';
import { Schema, Node as PMNode, Mark, DOMParser as PMDOMParser, DOMSerializer, NodeSpec, MarkSpec } from 'prosemirror-model';
import { keymap } from 'prosemirror-keymap';
import { history, undo as pmUndo, redo as pmRedo } from 'prosemirror-history';
import { baseKeymap } from 'prosemirror-commands';

const BLOCK_TYPES = [
  { tag: 'p', label: 'Paragraph', color: 'blue' },
  { tag: 'verse', label: 'Verse', color: 'purple' },
  { tag: 'heading', label: 'Heading', color: 'orange' },
  { tag: 'title', label: 'Title', color: 'indigo' },
  { tag: 'subtitle', label: 'Subtitle', color: 'pink' },
  { tag: 'footnote', label: 'Footnote', color: 'green' },
  { tag: 'trailer', label: 'Trailer', color: 'teal' },
  { tag: 'ignore', label: 'Ignore', color: 'gray' },
];

const BLOCK_TYPE_COLORS: Record<string, string> = {
  'p': 'border-blue-400',
  'verse': 'border-purple-400',
  'heading': 'border-orange-400',
  'title': 'border-indigo-400',
  'subtitle': 'border-pink-400',
  'footnote': 'border-green-400',
  'trailer': 'border-teal-400',
  'ignore': 'border-gray-300',
};

// Nodes are the basic pieces of the document.
const nodes: Record<string, NodeSpec> = {
  doc: {
    content: 'block+',
  },
  block: {
    content: 'inline*',
    attrs: {
      // The block type.
      type: { default: 'p' },
      // The text that this block belongs to
      text: { default: null },
      // Slug ID
      n: { default: null },
      // Footnote mark
      mark: { default: null },
      lang: { default: null },
      // If true, merge this block with the next when publishing at ext.
      merge_next: { default: false },
    },
    group: 'block',
    code: true,
    preserveWhitespace: 'full',
    parseDOM: [
      {
        // matched XML tags
        tag: 'p, verse, heading, title, subtitle, footnote, trailer, ignore',
        preserveWhitespace: 'full',
        getAttrs(dom: HTMLElement) {
          return {
            type: dom.tagName.toLowerCase(),
            text: dom.getAttribute('text'),
            n: dom.getAttribute('n'),
            mark: dom.getAttribute('mark'),
            lang: dom.getAttribute('lang'),
            merge_next: dom.getAttribute('merge-next') === 'true',
          };
        },
      },
    ],
    toDOM(node: PMNode) {
      const attrs: Record<string, string> = {};
      if (node.attrs.text) attrs.text = node.attrs.text;
      if (node.attrs.n) attrs.n = node.attrs.n;
      if (node.attrs.mark) attrs.mark = node.attrs.mark;
      if (node.attrs.lang) attrs.lang = node.attrs.lang;
      if (node.attrs.merge_next) attrs['merge-next'] = 'true';

      // format: [tag, attrs, "hole" where children should be inserted]
      return [node.attrs.type || 'p', attrs, 0];
    },
  },
  text: {
    group: 'inline',
  },
};

// Marks are labels attached to text.
const marks: Record<string, MarkSpec> = {
  error: {
    parseDOM: [{ tag: 'error' }],
    toDOM() {
      return ['span', { class: 'pm-error' }, 0];
    },
  },
  fix: {
    parseDOM: [{ tag: 'fix' }],
    toDOM() {
      return ['span', { class: 'pm-fix' }, 0];
    },
  },
  ref: {
    parseDOM: [{ tag: 'ref' }],
    toDOM() {
      return ['span', { class: 'pm-ref' }, 0];
    },
  },
  flag: {
    parseDOM: [{ tag: 'flag' }],
    toDOM() {
      return ['span', { class: 'pm-flag' }, 0];
    },
  },
};

const customSchema = new Schema({ nodes, marks });

function createBlockBelow(state: EditorState, dispatch?: (tr: Transaction) => void): boolean {
  const { $from } = state.selection;
  const currentBlock = $from.node($from.depth);

  if (currentBlock.type.name !== 'block') {
    return false;
  }

  if (dispatch) {
    const blockPos = $from.before($from.depth);
    const afterPos = blockPos + currentBlock.nodeSize;
    const newBlock = state.schema.nodes.block.create({ type: 'p' });
    const tr = state.tr.insert(afterPos, newBlock);
    tr.setSelection(Selection.near(tr.doc.resolve(afterPos + 1)));
    dispatch(tr);
  }

  return true;
}

class BlockView {
  dom: HTMLElement;
  contentDOM: HTMLElement;
  node: PMNode;
  view: EditorView;
  getPos: () => number | undefined;
  controlsDOM: HTMLElement;
  typeSelect: HTMLSelectElement;
  textInput: HTMLInputElement;
  textLabel: HTMLSpanElement;
  nInput: HTMLInputElement;
  nLabel: HTMLSpanElement;
  markInput: HTMLInputElement;
  mergeCheckbox: HTMLInputElement;
  mergeLabel: HTMLLabelElement;
  editor: any; // ProofingEditor instance

  constructor(node: PMNode, view: EditorView, getPos: () => number | undefined, editor: any) {
    this.node = node;
    this.view = view;
    this.getPos = getPos;
    this.editor = editor;

    // Register this BlockView with the editor
    if (editor.blockViews) {
      editor.blockViews.add(this);
    }

    // Ensure type is valid (default to 'p' if not set)
    const blockType = node.attrs.type || 'p';

    // Create wrapper
    this.dom = document.createElement('div');
    this.dom.className = `border-l-4 pl-4 mb-3 transition-colors ${BLOCK_TYPE_COLORS[blockType] || 'border-gray-400'}`;
    if (node.attrs.merge_next) {
      this.dom.classList.add('bg-yellow-50', '!border-dashed');
    }

    // Create controls area
    this.controlsDOM = document.createElement('div');
    this.controlsDOM.className = 'flex gap-1 mb-1 px-1.5 py-1 text-xs text-slate-500 items-center bg-slate-50 rounded leading-tight';

    // Type dropdown
    this.typeSelect = document.createElement('select');
    this.typeSelect.className = 'border border-slate-300 bg-white text-xs font-medium cursor-pointer hover:bg-slate-100 rounded px-1 py-0';
    const currentType = node.attrs.type || 'p';
    BLOCK_TYPES.forEach(bt => {
      const option = document.createElement('option');
      option.value = bt.tag;
      option.textContent = bt.label;
      if (bt.tag === currentType) {
        option.selected = true;
      }
      this.typeSelect.appendChild(option);
    });
    this.typeSelect.addEventListener('change', () => this.updateNodeAttr('type', this.typeSelect.value));
    this.controlsDOM.appendChild(this.typeSelect);

    // Text field
    this.textLabel = document.createElement('span');
    this.textLabel.className = 'text-slate-400 text-[11px] ml-1';
    this.textLabel.textContent = 'text=';
    this.textLabel.style.display = this.editor.showAdvancedOptions ? '' : 'none';
    this.controlsDOM.appendChild(this.textLabel);

    this.textInput = document.createElement('input');
    this.textInput.type = 'text';
    this.textInput.value = node.attrs.text || '';
    this.textInput.placeholder = 'text';
    this.textInput.className = 'border border-slate-300 bg-transparent text-xs text-slate-600 w-20 px-1 py-0 hover:bg-slate-100 rounded';
    this.textInput.style.display = this.editor.showAdvancedOptions ? '' : 'none';
    this.textInput.addEventListener('change', () => this.updateNodeAttr('text', this.textInput.value || null));
    this.controlsDOM.appendChild(this.textInput);

    // N field (if not footnote)
    if (node.attrs.type !== 'footnote') {
      this.nLabel = document.createElement('span');
      this.nLabel.className = 'text-slate-400 text-[11px] ml-1';
      this.nLabel.textContent = 'n=';
      this.nLabel.style.display = this.editor.showAdvancedOptions ? '' : 'none';
      this.controlsDOM.appendChild(this.nLabel);

      this.nInput = document.createElement('input');
      this.nInput.type = 'text';
      this.nInput.value = node.attrs.n || '';
      this.nInput.placeholder = '#';
      this.nInput.className = 'border border-slate-300 bg-transparent font-mono text-xs text-slate-600 w-12 px-1 py-0 hover:bg-slate-100 rounded';
      this.nInput.style.display = this.editor.showAdvancedOptions ? '' : 'none';
      this.nInput.addEventListener('change', () => this.updateNodeAttr('n', this.nInput.value || null));
      this.controlsDOM.appendChild(this.nInput);
    }

    // Mark field (if footnote)
    if (node.attrs.type === 'footnote') {
      const markLabel = document.createElement('span');
      markLabel.className = 'text-slate-400 text-[11px] ml-1';
      markLabel.textContent = 'mark=';
      this.controlsDOM.appendChild(markLabel);

      this.markInput = document.createElement('input');
      this.markInput.type = 'text';
      this.markInput.value = node.attrs.mark || '';
      this.markInput.placeholder = 'mark';
      this.markInput.className = 'border border-slate-300 bg-transparent font-mono text-xs text-slate-600 w-16 px-1 py-0 hover:bg-slate-100 rounded';
      this.markInput.addEventListener('change', () => this.updateNodeAttr('mark', this.markInput.value || null));
      this.controlsDOM.appendChild(this.markInput);
    }

    // Merge checkbox
    this.mergeLabel = document.createElement('label');
    this.mergeLabel.className = 'flex items-center gap-0.5 cursor-pointer hover:bg-slate-100 px-1 rounded ml-1';
    this.mergeLabel.style.display = this.editor.showAdvancedOptions ? '' : 'none';

    this.mergeCheckbox = document.createElement('input');
    this.mergeCheckbox.type = 'checkbox';
    this.mergeCheckbox.className = 'w-3 h-3';
    this.mergeCheckbox.checked = node.attrs.merge_next || false;
    this.mergeCheckbox.addEventListener('change', () => this.updateNodeAttr('merge_next', this.mergeCheckbox.checked));

    const mergeText = document.createElement('span');
    mergeText.className = 'text-[11px]';
    mergeText.textContent = 'merge next';

    this.mergeLabel.appendChild(this.mergeCheckbox);
    this.mergeLabel.appendChild(mergeText);
    this.controlsDOM.appendChild(this.mergeLabel);

    this.dom.appendChild(this.controlsDOM);

    // Create content area
    this.contentDOM = document.createElement('div');
    this.contentDOM.className = 'w-full text-base p-2 border border-slate-200 bg-white rounded font-normal min-h-[3rem] focus:outline-none focus:ring-2 focus:ring-blue-400 whitespace-pre-wrap';
    this.contentDOM.contentEditable = 'true';
    this.dom.appendChild(this.contentDOM);
  }

  updateNodeAttr(name: string, value: any) {
    const pos = this.getPos();
    if (pos === undefined) return;

    const tr = this.view.state.tr.setNodeMarkup(pos, undefined, {
      ...this.node.attrs,
      [name]: value,
    });
    this.view.dispatch(tr);

    // Update visual classes if type changed
    if (name === 'type') {
      this.dom.className = `border-l-4 pl-4 mb-3 transition-colors ${BLOCK_TYPE_COLORS[value] || 'border-gray-400'}`;
      if (this.node.attrs.merge_next) {
        this.dom.classList.add('bg-yellow-50', '!border-dashed');
      }
    }

    // Update visual classes if merge_next changed
    if (name === 'merge_next') {
      if (value) {
        this.dom.classList.add('bg-yellow-50', '!border-dashed');
      } else {
        this.dom.classList.remove('bg-yellow-50', '!border-dashed');
      }
    }
  }

  update(node: PMNode) {
    if (node.type !== this.node.type) return false;

    this.node = node;

    // Update DOM classes to match node type
    const blockType = node.attrs.type || 'p';
    this.dom.className = `border-l-4 pl-4 mb-3 transition-colors ${BLOCK_TYPE_COLORS[blockType] || 'border-gray-400'}`;
    if (node.attrs.merge_next) {
      this.dom.classList.add('bg-yellow-50', '!border-dashed');
    }

    // Update controls to match new node attrs
    if (this.typeSelect.value !== blockType) {
      this.typeSelect.value = blockType;
    }
    if (this.textInput.value !== (node.attrs.text || '')) {
      this.textInput.value = node.attrs.text || '';
    }
    if (this.nInput && this.nInput.value !== (node.attrs.n || '')) {
      this.nInput.value = node.attrs.n || '';
    }
    if (this.markInput && this.markInput.value !== (node.attrs.mark || '')) {
      this.markInput.value = node.attrs.mark || '';
    }
    if (this.mergeCheckbox.checked !== node.attrs.merge_next) {
      this.mergeCheckbox.checked = node.attrs.merge_next;
    }

    return true;
  }

  stopEvent(event: Event) {
    // Allow all events within the contentDOM (for editing)
    // but prevent events in the controls from affecting ProseMirror
    return this.controlsDOM.contains(event.target as Node);
  }

  ignoreMutation(mutation: MutationRecord) {
    // Ignore mutations in controls
    if (mutation.type === 'attributes' && mutation.target !== this.contentDOM) {
      return true;
    }
    return false;
  }

  updateAdvancedOptionsVisibility() {
    const show = this.editor.showAdvancedOptions;
    this.textLabel.style.display = show ? '' : 'none';
    this.textInput.style.display = show ? '' : 'none';
    if (this.nLabel) {
      this.nLabel.style.display = show ? '' : 'none';
    }
    if (this.nInput) {
      this.nInput.style.display = show ? '' : 'none';
    }
    this.mergeLabel.style.display = show ? '' : 'none';
  }

  destroy() {
    // Unregister this BlockView from the editor
    if (this.editor.blockViews) {
      this.editor.blockViews.delete(this);
    }
  }
}

// Parse XML content to ProseMirror document
// XML is always rooted in a <page> tag containing block elements
function parseXMLToDoc(xmlString: string, schema: Schema): PMNode {
  // Handle empty content
  if (!xmlString || xmlString.trim() === '') {
    return schema.node('doc', null, [schema.node('block', { type: 'p' })]);
  }

  const parser = new DOMParser();
  const xmlDoc = parser.parseFromString(xmlString, 'text/xml');

  // Check for parse errors
  const parseError = xmlDoc.querySelector('parsererror');
  if (parseError) {
    console.error('[parseXMLToDoc] XML parse error:', parseError.textContent);
    throw new Error(`Failed to parse XML: ${parseError.textContent}`);
  }

  const blocks: PMNode[] = [];
  const pageElement = xmlDoc.documentElement;

  // Verify it's a <page> element
  if (pageElement.tagName.toLowerCase() !== 'page') {
    throw new Error(`Expected <page> root element, got <${pageElement.tagName}>`);
  }

  // Parse all child block elements
  for (let i = 0; i < pageElement.children.length; i++) {
    const elem = pageElement.children[i];
    const type = elem.tagName.toLowerCase();

    // Extract attributes
    const attrs: any = { type };
    if (elem.hasAttribute('text')) attrs.text = elem.getAttribute('text');
    if (elem.hasAttribute('n')) attrs.n = elem.getAttribute('n');
    if (elem.hasAttribute('mark')) attrs.mark = elem.getAttribute('mark');
    if (elem.hasAttribute('lang')) attrs.lang = elem.getAttribute('lang');
    if (elem.getAttribute('merge-next') === 'true') attrs.merge_next = true;

    // Parse inline content
    const content = parseInlineContent(elem, schema);

    blocks.push(schema.node('block', attrs, content));
  }

  if (blocks.length === 0) {
    blocks.push(schema.node('block', { type: 'p' }));
  }

  return schema.node('doc', null, blocks);
}

function parseInlineContent(elem: Element, schema: Schema): PMNode[] {
  const result: PMNode[] = [];

  function serializeNode(node: Node): string {
    if (node.nodeType === Node.TEXT_NODE) {
      return node.textContent || '';
    } else if (node.nodeType === Node.ELEMENT_NODE) {
      const el = node as Element;
      const tagName = el.tagName.toLowerCase();
      const children = Array.from(node.childNodes).map(serializeNode).join('');
      return `<${tagName}>${children}</${tagName}>`;
    }
    return '';
  }

  function traverse(node: Node, marks: readonly Mark[] = []) {
    if (node.nodeType === Node.TEXT_NODE) {
      const text = node.textContent || '';
      if (text) {
        result.push(schema.text(text, marks));
      }
    } else if (node.nodeType === Node.ELEMENT_NODE) {
      const el = node as Element;
      const tagName = el.tagName.toLowerCase();

      // Check if it's a mark we want to render visually
      if (tagName === 'error' || tagName === 'fix' || tagName === 'flag' || tagName === 'ref') {
        const mark = schema.mark(tagName);
        const newMarks = mark.addToSet(marks);
        // Traverse children with the mark applied
        for (let i = 0; i < node.childNodes.length; i++) {
          traverse(node.childNodes[i], newMarks);
        }
      } else {
        // For other inline elements (like <a>, <b>, etc.), preserve them as text
        const serialized = serializeNode(node);
        if (serialized) {
          result.push(schema.text(serialized, marks));
        }
      }
    }
  }

  for (let i = 0; i < elem.childNodes.length; i++) {
    traverse(elem.childNodes[i]);
  }

  return result;
}

function serializeDocToXML(doc: PMNode): string {
  const parts: string[] = [];

  doc.forEach((block) => {
    const type = block.attrs.type || 'p';
    const attrs: string[] = [];

    if (block.attrs.text) attrs.push(`text="${escapeXML(block.attrs.text)}"`);
    if (block.attrs.n) attrs.push(`n="${escapeXML(block.attrs.n)}"`);
    if (block.attrs.mark) attrs.push(`mark="${escapeXML(block.attrs.mark)}"`);
    if (block.attrs.lang) attrs.push(`lang="${escapeXML(block.attrs.lang)}"`);
    if (block.attrs.merge_next) attrs.push('merge-next="true"');

    const attrsStr = attrs.length > 0 ? ' ' + attrs.join(' ') : '';
    const content = serializeInlineContent(block);

    parts.push(`<${type}${attrsStr}>${content}</${type}>`);
  });

  return `<page>\n${parts.join('\n')}\n</page>`;
}

function serializeInlineContent(node: PMNode): string {
  let result = '';

  node.forEach((child, _, index) => {
    if (child.isText) {
      let text = escapeXML(child.text || '');
      child.marks.forEach(mark => {
        text = `<${mark.type.name}>${text}</${mark.type.name}>`;
      });

      result += text;
    }
  });

  return result;
}

function escapeXML(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

// Schema for XML editing mode - a simple code editor
const xmlSchema = new Schema({
  nodes: {
    doc: {
      content: 'codeblock',
    },
    codeblock: {
      content: 'text*',
      group: 'block',
      code: true,
      preserveWhitespace: 'full',
      parseDOM: [{ tag: 'pre' }],
      toDOM() {
        return ['pre', { class: 'xml-code' }, 0];
      },
    },
    text: {
      group: 'inline',
    },
  },
  marks: {},
});

// Plugin to add XML syntax highlighting decorations
function xmlHighlightPlugin() {
  return new Plugin({
    state: {
      init(_, state) {
        return createXMLDecorations(state);
      },
      apply(tr, set, oldState, newState) {
        if (!tr.docChanged) return set;
        return createXMLDecorations(newState);
      },
    },
    props: {
      decorations(state) {
        return this.getState(state);
      },
    },
  });
}

function createXMLDecorations(state: EditorState): DecorationSet {
  const decorations: Decoration[] = [];
  const text = state.doc.textContent;

  const tagRegex = /<\/?([a-zA-Z][\w-]*)((?:\s+[\w-]+(?:="[^"]*")?)*)\s*\/?>/g;
  let match;

  while ((match = tagRegex.exec(text)) !== null) {
    // Positions need to account for document structure:
    // doc (pos 0) -> codeblock (pos 1) -> text content starts at pos 1
    // So we add 1 to convert text offsets to document positions
    const from = match.index + 1;
    const to = match.index + match[0].length + 1;

    decorations.push(
      Decoration.inline(from, to, {
        style: 'color: #60a5fa;', // Blue color for tags
      })
    );
  }

  return DecorationSet.create(state.doc, decorations);
}

export class XMLView {
  view: EditorView;
  schema: Schema;
  onChange?: () => void;

  constructor(element: HTMLElement, initialContent: string = '', onChange?: () => void) {
    this.schema = xmlSchema;
    this.onChange = onChange;

    const textNode = initialContent ? this.schema.text(initialContent) : undefined;
    const codeblock = this.schema.node('codeblock', null, textNode ? [textNode] : []);

    const state = EditorState.create({
      doc: this.schema.node('doc', null, [codeblock]),
      plugins: [
        history(),
        xmlHighlightPlugin(),
        keymap({ 'Mod-z': pmUndo, 'Mod-y': pmRedo }),
        keymap(baseKeymap),
      ],
    });

    // Create wrapper for styling
    const wrapper = document.createElement('div');
    wrapper.className = 'w-full h-full bg-gray-800 text-gray-300';
    element.appendChild(wrapper);

    this.view = new EditorView(wrapper, {
      state,
      dispatchTransaction: (transaction) => {
        const newState = this.view.state.apply(transaction);
        this.view.updateState(newState);

        if (transaction.docChanged && this.onChange) {
          this.onChange();
        }
      },
      attributes: {
        class: 'w-full h-full font-mono text-sm focus:outline-none',
        spellcheck: 'false',
      },
    });
  }

  getText(): string {
    return this.view.state.doc.textContent;
  }

  setText(text: string) {
    const textNode = text ? this.schema.text(text) : undefined;
    const codeblock = this.schema.node('codeblock', null, textNode ? [textNode] : []);
    const newState = EditorState.create({
      doc: this.schema.node('doc', null, [codeblock]),
      plugins: this.view.state.plugins,
    });
    this.view.updateState(newState);
  }

  focus() {
    this.view.focus();
  }

  getSelection(): { from: number; to: number; text: string } {
    const { from, to } = this.view.state.selection;
    const text = this.view.state.doc.textBetween(from, to, '\n');
    return { from, to, text };
  }

  replaceSelection(text: string) {
    const state = this.view.state;
    const { from, to } = state.selection;

    const tr = state.tr.insertText(text, from, to);
    const newState = state.apply(tr);
    this.view.updateState(newState);
    this.view.focus();

    if (this.onChange) {
      this.onChange();
    }
  }

  undo() {
    pmUndo(this.view.state, this.view.dispatch);
  }

  redo() {
    pmRedo(this.view.state, this.view.dispatch);
  }

  destroy() {
    this.view.destroy();
  }
}

export default class {
  view: EditorView;
  schema: Schema;
  onChange?: () => void;
  showAdvancedOptions: boolean;
  blockViews: Set<BlockView>;

  constructor(element: HTMLElement, initialContent: string = '', onChange?: () => void, showAdvancedOptions: boolean = false) {
    this.schema = customSchema;
    this.onChange = onChange;
    this.showAdvancedOptions = showAdvancedOptions;
    this.blockViews = new Set();

    let doc;
    try {
      doc = parseXMLToDoc(initialContent, this.schema);
    } catch  (error) {
      doc = this.schema.node('doc', null, [this.schema.node('block', { type: 'p' })]);
    }

    const state = EditorState.create({
      doc,
      plugins: [
        history(),
        keymap({ 'Mod-z': pmUndo, 'Mod-y': pmRedo, 'Shift-Enter': createBlockBelow }),
        keymap(baseKeymap),
      ],
    });

    this.view = new EditorView(element, {
      state,
      nodeViews: {
        block: (node, view, getPos) => new BlockView(node, view, getPos as () => number, this),
      },
      dispatchTransaction: (transaction) => {
        const newState = this.view.state.apply(transaction);
        this.view.updateState(newState);

        if (transaction.docChanged && this.onChange) {
          this.onChange();
        }
      },
    });
  }

  getText(): string {
    return serializeDocToXML(this.view.state.doc);
  }

  setText(text: string) {
    const newDoc = parseXMLToDoc(text, this.schema);
    const newState = EditorState.create({
      doc: newDoc,
      plugins: this.view.state.plugins,
    });
    this.view.updateState(newState);
  }

  focus() {
    this.view.focus();
  }

  getSelection(): { from: number; to: number; text: string } {
    const { from, to } = this.view.state.selection;
    const text = this.view.state.doc.textBetween(from, to, '\n');
    return { from, to, text };
  }

  replaceSelection(text: string) {
    const state = this.view.state;
    const { from, to } = state.selection;

    const tr = state.tr.insertText(text, from, to);
    const newState = state.apply(tr);
    this.view.updateState(newState);
    this.view.focus();

    if (this.onChange) {
      this.onChange();
    }
  }

  toggleMark(markType: 'error' | 'fix' | 'flag' | 'ref') {
    console.log("togglemark", markType);
    const { state, dispatch } = this.view;
    const { from, to } = state.selection;

    const mark = this.schema.marks[markType];
    if (!mark) return;

    const hasMark = state.doc.rangeHasMark(from, to, mark);
    if (hasMark) {
      const tr = state.tr.removeMark(from, to, mark);
      dispatch(tr);
    } else {
      const tr = state.tr.addMark(from, to, mark.create());
      dispatch(tr);
    }
  }

  insertBlock() {
    const { state, dispatch } = this.view;
    const { $from } = state.selection;

    // Find the current block
    let blockDepth = $from.depth;
    while (blockDepth > 0 && state.doc.resolve($from.pos).node(blockDepth).type.name !== 'block') {
      blockDepth--;
    }

    if (blockDepth === 0) return;

    const blockPos = $from.before(blockDepth);
    const currentBlock = $from.node(blockDepth);
    const afterPos = blockPos + currentBlock.nodeSize;

    // Create a new empty block with default type 'p'
    const newBlock = this.schema.nodes.block.create({ type: 'p' });
    const tr = state.tr.insert(afterPos, newBlock);

    // Move cursor to the new block
    tr.setSelection(Selection.near(tr.doc.resolve(afterPos + 1)));
    dispatch(tr);

    if (this.onChange) {
      this.onChange();
    }
  }

  deleteActiveBlock() {
    const { state, dispatch } = this.view;
    const { $from } = state.selection;

    // Find the current block
    let blockDepth = $from.depth;
    while (blockDepth > 0 && state.doc.resolve($from.pos).node(blockDepth).type.name !== 'block') {
      blockDepth--;
    }

    if (blockDepth === 0) return;

    // Don't allow deleting if it's the only block
    if (state.doc.childCount === 1) {
      console.log('Cannot delete the only block');
      return;
    }

    const blockPos = $from.before(blockDepth);
    const currentBlock = $from.node(blockDepth);
    const tr = state.tr.delete(blockPos, blockPos + currentBlock.nodeSize);

    dispatch(tr);

    if (this.onChange) {
      this.onChange();
    }
  }

  undo() {
    pmUndo(this.view.state, this.view.dispatch);
  }

  redo() {
    pmRedo(this.view.state, this.view.dispatch);
  }

  setShowAdvancedOptions(show: boolean) {
    this.showAdvancedOptions = show;

    // Update all existing BlockViews
    this.blockViews.forEach(blockView => {
      blockView.updateAdvancedOptionsVisibility();
    });
  }

  destroy() {
    this.view.destroy();
  }
}
