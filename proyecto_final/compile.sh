#!/bin/bash

export TEXINPUTS=./styles//:

pdflatex -interaction=nonstopmode -output-directory=output memtfg.tex

if grep -q '\\bibdata' output/memtfg.aux 2>/dev/null; then
    bibtex output/memtfg
    pdflatex -interaction=nonstopmode -output-directory=output memtfg.tex
fi

pdflatex -interaction=nonstopmode -output-directory=output memtfg.tex

mv output/memtfg.log log/

echo "PDF generado en output/memtfg.pdf"
