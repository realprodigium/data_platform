---
name: nothing-design
description: This skill should be used when the user explicitly says "Nothing style", "Nothing design", "/nothing-design", or directly asks to use/apply the Nothing design system. NEVER trigger automatically for generic UI or design tasks.
version: 3.0.0
allowed-tools: [Read, Write, Edit, Glob, Grep]
---

# Nothing-Inspired UI/UX Design System

A senior product designer's toolkit trained in Swiss typography, industrial design (Braun, Teenage Engineering), and modern interface craft. Monochromatic, typographically driven, information-dense without clutter. Dark and light mode with equal rigor.

**Before starting any design work, declare which Google Fonts are required and how to load them. Never assume fonts are already available.**

---

## 1. DESIGN PHILOSOPHY

- **Subtract, don't add.** Every element must earn its pixel. Default to removal.
- **Structure is ornament.** Expose the grid, the data, the hierarchy itself.
- **Monochrome is the canvas.** Color is an event, not a default — except when encoding data status.
- **Type does the heavy lifting.** Scale, weight, and spacing create hierarchy — not color, not icons, not borders.
- **Both modes are first-class.** Dark mode: OLED black. Light mode: warm off-white.
- **Industrial warmth.** Technical and precise, but never cold.

---

## 2. CRAFT RULES — HOW TO COMPOSE

### 2.1 Visual Hierarchy: The Three-Layer Rule

Every screen has exactly **three layers of importance.**

| Layer | What | How |
|-------|------|-----|
| **Primary** | The ONE thing the user sees first. | Doto or Space Grotesk at display size. |
| **Secondary** | Supporting context. | Space Grotesk grouped tight to primary. |
| **Tertiary** | Metadata and system info. | Space Mono, ALL CAPS, pushed to edges. |

---

### 2.2 Font Discipline

- Max 2 font families  
- Max 3 sizes  
- Max 2 weights  

---

### 2.3 Spacing as Meaning

Tight (4–8px) → Same element  
Medium (16px) → Same group  
Wide (32–48px) → New group  
Vast (64–96px) → New context  

---

### 2.4 Container Strategy

1. Spacing  
2. Divider  
3. Border  
4. Card  

---

### 2.5 Color as Hierarchy

- Display (100%)  
- Primary (90%)  
- Secondary (60%)  
- Disabled (40%)  

Red = interrupt only.

---

### 2.6 Consistency vs. Variance

- Be consistent in system rules  
- Break pattern once per screen  

---

### 2.7 Compositional Balance

- Asymmetry  
- Edge anchoring  
- Negative space  

---

### 2.8 The Nothing Vibe

- Emptiness  
- Precision  
- Data as visual  
- Mechanical honesty  
- One surprise  
- Percussive interaction  

---

### 2.9 Data Visualization Forms

- Hero number  
- Progress bar  
- Rings  
- Inline bars  
- Number-only  
- Sparkline  
- Stat row  

---

## 3. ANTI-PATTERNS

- No gradients  
- No shadows  
- No skeleton loaders  
- No toasts  
- No mascots  
- No zebra tables  
- No filled icons  
- No parallax  
- No bounce easing  
- No large radius  
- No color-first data  

---

## 4. WORKFLOW

1. Declare fonts  
2. Ask mode  
3. Define hierarchy  
4. Compose  
5. Apply tokens  
6. Build  
7. Adapt  
