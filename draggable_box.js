(function (root, factory) {
    const api = factory();

    if (typeof module === 'object' && module.exports) {
        module.exports = api;
    }

    root.draggableBoxUtils = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';

    const iconMap = new WeakMap();

    function adoptScripts(parent) {
        const scripts = parent.querySelectorAll('script');
        scripts.forEach((oldScript) => {
            const newScript = document.createElement('script');

            if (oldScript.type) {
                newScript.type = oldScript.type;
            }

            if (oldScript.src) {
                newScript.src = oldScript.src;
            } else {
                newScript.textContent = oldScript.textContent;
            }

            parent.appendChild(newScript);
            oldScript.remove();
        });
    }

    function attachDraggableBoxIcon(iconElement, contentElement, title = 'Chart') {
        if (iconMap.has(iconElement)) {
            return;
        }

        const data = {
            box: null,
            header: null,
            isCreated: false,
            isPermanentlyShown: false,
            isDown: false,
            offsetX: 0,
            offsetY: 0
        };
        iconMap.set(iconElement, data);

        function createBox() {
            data.isCreated = true;

            const box = document.createElement('div');
            box.style.position = 'absolute';
            box.style.width = '500px';
            box.style.height = '250px';
            box.style.zIndex = 900;
            box.style.backgroundColor = 'var(--bs-body-bg)';
            box.style.color = 'var(--bs-body-color)';
            box.style.border = '1px solid var(--bs-border-color)';
            box.style.display = 'none';

            const header = document.createElement('div');
            header.style.backgroundColor = 'var(--bs-tertiary-bg)';
            header.style.color = 'var(--bs-body-color)';
            header.style.padding = '2px 2px 2px 5px';
            header.style.borderBottom = '1px solid var(--bs-border-color)';
            header.style.cursor = 'move';
            header.style.fontSize = 'small';
            header.innerHTML = `<strong>${title}</strong>`;

            const contentWrapper = document.createElement('div');
            contentWrapper.style.padding = '10px';
            contentWrapper.style.height = '90%';
            contentWrapper.appendChild(contentElement);

            contentElement.style.height = '100%';
            adoptScripts(contentWrapper);

            box.appendChild(header);
            box.appendChild(contentWrapper);
            document.body.appendChild(box);

            data.box = box;
            data.header = header;

            header.addEventListener('mousedown', (event) => {
                event.preventDefault();
                data.isDown = true;
                data.offsetX = event.pageX - box.offsetLeft;
                data.offsetY = event.pageY - box.offsetTop;
                document.body.style.userSelect = 'none';
            });

            document.addEventListener('mousemove', (event) => {
                if (!data.isDown) return;
                event.preventDefault();
                box.style.left = `${event.pageX - data.offsetX}px`;
                box.style.top = `${event.pageY - data.offsetY}px`;
            });

            document.addEventListener('mouseup', () => {
                data.isDown = false;
                document.body.style.userSelect = '';
            });
        }

        function showBox() {
            if (!data.box) return;
            const rect = iconElement.getBoundingClientRect();
            data.box.style.top = `${rect.bottom + window.scrollY + 5}px`;
            data.box.style.left = `${rect.left + window.scrollX}px`;
            data.box.style.display = 'block';
        }

        function hideBox() {
            if (!data.box) return;
            data.box.style.display = 'none';
        }

        function onMouseEnter() {
            if (!data.isPermanentlyShown) {
                if (!data.isCreated) createBox();
                showBox();
            }
        }

        function onMouseLeave() {
            if (!data.isPermanentlyShown) {
                hideBox();
            }
        }

        function onClick() {
            data.isPermanentlyShown = !data.isPermanentlyShown;
            if (data.isPermanentlyShown) {
                if (!data.isCreated) createBox();
                showBox();
            } else {
                hideBox();
            }
        }

        iconElement.addEventListener('mouseenter', onMouseEnter);
        iconElement.addEventListener('mouseleave', onMouseLeave);
        iconElement.addEventListener('click', onClick);
    }

    return {
        attachDraggableBoxIcon
    };
}));
