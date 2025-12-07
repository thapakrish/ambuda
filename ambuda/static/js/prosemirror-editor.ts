import { EditorState, Plugin } from 'prosemirror-state';
import { EditorView, Decoration, DecorationSet } from 'prosemirror-view';
import { Schema, DOMParser, DOMSerializer } from 'prosemirror-model';
import { schema as basicSchema } from 'prosemirror-schema-basic';
import { keymap } from 'prosemirror-keymap';
import { history, undo, redo } from 'prosemirror-history';
import { baseKeymap } from 'prosemirror-commands';

interface TagInfo {
  tag: string;
  from: number;
  to: number;
  type: 'opening' | 'closing' | 'self-closing';
}

/** Finds all XML tags and validates them. */
function findHtmlTags(doc: any): DecorationSet {
  const decorations: Decoration[] = [];
  const tags: TagInfo[] = [];

  // First pass: collect all tags with their positions and types
  doc.descendants((node: any, pos: number) => {
    if (node.isText && node.text) {
      const htmlTagRegex = /<\/?([a-zA-Z][a-zA-Z0-9]*)[^>]*\/?>/g;
      let match;
      while ((match = htmlTagRegex.exec(node.text)) !== null) {
        const fullTag = match[0];
        const tagName = match[1];
        const from = pos + match.index;
        const to = from + fullTag.length;

        let type: 'opening' | 'closing' | 'self-closing';
        if (fullTag.endsWith('/>')) {
          type = 'self-closing';
        } else if (fullTag.startsWith('</')) {
          type = 'closing';
        } else {
          type = 'opening';
        }

        tags.push({
          tag: tagName, from, to, type,
        });
      }
    }
  });

  // Second pass: match tags using a stack to track nesting
  const stack: { tag: string; index: number }[] = [];
  const matched = new Set<number>();

  tags.forEach((tagInfo, index) => {
    if (tagInfo.type === 'self-closing') {
      // Self-closing tags are always valid.
      matched.add(index);
    } else if (tagInfo.type === 'opening') {
      // Push and wait for a match.
      stack.push({ tag: tagInfo.tag, index });
    } else if (tagInfo.type === 'closing') {
      if (stack.length > 0 && stack[stack.length - 1].tag === tagInfo.tag) {
        // Match -- mark both as matched.
        const openingIndex = stack.pop()!.index;
        matched.add(openingIndex);
        matched.add(index);
      }
    }
  });

  // Third pass: create decorations based on match status
  tags.forEach((tagInfo, index) => {
    const isMatched = matched.has(index);
    decorations.push(
      Decoration.inline(tagInfo.from, tagInfo.to, {
        class: isMatched ? 'pm-html-tag' : 'pm-html-tag-error',
      }),
    );
  });

  return DecorationSet.create(doc, decorations);
}

/** Highlights HTML tags in the editor. */
function htmlTagHighlightPlugin(): Plugin {
  return new Plugin({
    state: {
      init(_, { doc }) {
        return findHtmlTags(doc);
      },
      apply(tr, oldDecorations) {
        if (tr.docChanged) {
          return findHtmlTags(tr.doc);
        }
        return oldDecorations.map(tr.mapping, tr.doc);
      },
    },
    props: {
      decorations(state) {
        return this.getState(state);
      },
    },
  });
}

export class ProofingEditor {
  view: EditorView;

  schema: Schema;

  onChange?: () => void;

  constructor(element: HTMLElement, initialContent: string = '', onChange?: () => void) {
    this.schema = basicSchema;
    this.onChange = onChange;

    const state = EditorState.create({
      doc: this.createDocFromText(initialContent),
      plugins: [
        history(),
        keymap({ 'Mod-z': undo, 'Mod-y': redo }),
        keymap(baseKeymap),
        // Plugins
        htmlTagHighlightPlugin(),
      ],
    });

    this.view = new EditorView(element, {
      state,
      dispatchTransaction: (transaction) => {
        const newState = this.view.state.apply(transaction);
        this.view.updateState(newState);

        // Call onChange callback if the document changed
        if (transaction.docChanged && this.onChange) {
          this.onChange();
        }
      },
    });
  }

  private createDocFromText(text: string) {
    const lines = text.split('\n');
    const paragraphs = lines.map((line) => {
      // Always create a text node, even for empty lines, to preserve blank lines
      const content = line.length > 0 ? [this.schema.text(line)] : [];
      return this.schema.node('paragraph', null, content);
    });
    return this.schema.node('doc', null, paragraphs.length > 0 ? paragraphs : [this.schema.node('paragraph')]);
  }

  getText(): string {
    const { doc } = this.view.state;
    const paragraphs: string[] = [];
    doc.forEach((node) => {
      if (node.type.name === 'paragraph') {
        paragraphs.push(node.textContent);
      }
    });
    return paragraphs.join('\n');
  }

  setText(text: string) {
    const newDoc = this.createDocFromText(text);
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
    const { from, to } = this.view.state.selection;
    const tr = this.view.state.tr.insertText(text, from, to);
    this.view.dispatch(tr);
    this.view.focus();
  }

  destroy() {
    this.view.destroy();
  }
}
